import os
import pika
import json
import time
from minio import Minio
from datetime import datetime
import logging
import subprocess
from threading import Thread, Lock
from collections import deque
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

# --- Configuration ---
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'Noureldin')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD', 'Nour123#')
MINIO_URL = os.environ.get('MINIO_URL', 'minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'Noureldin')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'Nour123#')

# --- FastAPI App Initialization ---
app = FastAPI(title="AI Result Handler")
app.mount("/static", StaticFiles(directory="static"), name="static")

# --- In-Memory Data Storage (Thread-Safe) ---
data_lock = Lock()
MAX_LOG_ENTRIES = 100
MAX_EVENTS = 50
system_logs = deque(maxlen=MAX_LOG_ENTRIES)
detected_events = deque(maxlen=MAX_EVENTS)

# --- Logging Setup ---
class DequeLogHandler(logging.Handler):
    def __init__(self, deque_instance):
        super().__init__()
        self.deque = deque_instance

    def emit(self, record):
        log_entry = self.format(record)
        with data_lock:
            self.deque.append(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {log_entry}")

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
deque_handler = DequeLogHandler(system_logs)
logging.getLogger().addHandler(deque_handler)
logging.info("--- AI Result Handler Service Starting ---")

# --- MinIO and Local Storage Setup ---
minio_client = Minio(MINIO_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
RAW_CHUNK_BUCKET = 'video-chunks'
PROCESSED_VIDEO_BUCKET = 'processed-videos'
THUMBNAIL_BUCKET = 'thumbnails'
LOCAL_VIDEO_DIR = 'local_videos'
LOCAL_THUMBNAIL_DIR = 'local_thumbnails'
os.makedirs(LOCAL_VIDEO_DIR, exist_ok=True)
os.makedirs(LOCAL_THUMBNAIL_DIR, exist_ok=True)

for bucket in [RAW_CHUNK_BUCKET, PROCESSED_VIDEO_BUCKET, THUMBNAIL_BUCKET]:
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)
        logging.info(f"Created MinIO bucket: {bucket}")

# --- State Management ---
camera_data = {}

# --- Core Logic Functions (remain the same) ---
def get_camera_id(body):
    return body.get('camera_id')

def download_chunk(object_name):
    try:
        response = minio_client.get_object(RAW_CHUNK_BUCKET, object_name)
        file_path = os.path.join('/tmp', object_name)
        
        # Ensure the destination directory exists before writing the file
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'wb') as file:
            file.write(response.read())
        return file_path
    except Exception as e:
        logging.error(f"Failed to download chunk {object_name}: {e}")
        return None

def generate_thumbnail(video_path, thumbnail_filename):
    thumbnail_path = os.path.join(LOCAL_THUMBNAIL_DIR, thumbnail_filename)
    try:
        subprocess.run([
            'ffmpeg', '-i', video_path, '-ss', '00:00:01', '-vframes', '1',
            '-vf', 'scale=320:-1', thumbnail_path, '-y'
        ], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(f"Generated thumbnail: {thumbnail_filename}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"FFmpeg failed to generate thumbnail for {video_path}: {e.stderr.decode()}")
        return False
    except Exception as e:
        logging.error(f"Error generating thumbnail for {video_path}: {e}")
        return False

def combine_chunks_and_upload(camera_id, chunks_to_process):
    logging.info(f"[{camera_id}] Combining {len(chunks_to_process)} chunks for processing.")
    local_chunk_paths = [path for chunk in chunks_to_process if (path := download_chunk(chunk['object_name'])) is not None]

    if not local_chunk_paths:
        logging.error(f"[{camera_id}] No chunks were downloaded. Aborting combination.")
        return

    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_filename = f"incident_{camera_id}_{timestamp_str}.mp4"
    output_path = os.path.join(LOCAL_VIDEO_DIR, output_filename)
    
    file_list_path = f"/tmp/file_list_{camera_id}.txt"
    with open(file_list_path, 'w') as f:
        for path in local_chunk_paths:
            f.write(f"file '{path}'\n")

    try:
        subprocess.run(['ffmpeg', '-f', 'concat', '-safe', '0', '-i', file_list_path, '-c', 'copy', output_path, '-y'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(f"[{camera_id}] Successfully combined video: {output_filename}")
        minio_client.fput_object(PROCESSED_VIDEO_BUCKET, output_filename, output_path)
        logging.info(f"[{camera_id}] Uploaded combined video to MinIO bucket '{PROCESSED_VIDEO_BUCKET}'.")

        thumbnail_filename = output_filename.replace('.mp4', '.jpg')
        if generate_thumbnail(output_path, thumbnail_filename):
            minio_client.fput_object(THUMBNAIL_BUCKET, thumbnail_filename, os.path.join(LOCAL_THUMBNAIL_DIR, thumbnail_filename))
            logging.info(f"Uploaded thumbnail to MinIO: {thumbnail_filename}")

        with data_lock:
            detected_events.append({
                'camera_id': camera_id, 'timestamp': datetime.now().isoformat(),
                'video_filename': output_filename, 'video_url': f'/videos/{output_filename}',
                'thumbnail_url': f'/thumbnails/{thumbnail_filename}'
            })

    except subprocess.CalledProcessError as e:
        logging.error(f"[{camera_id}] FFmpeg failed during chunk combination: {e.stderr.decode()}")
    except Exception as e:
        logging.error(f"[{camera_id}] An error occurred during video processing: {e}")
    finally:
        for path in local_chunk_paths: os.remove(path)
        if os.path.exists(file_list_path): os.remove(file_list_path)

def process_message(body):
    camera_id = get_camera_id(body)
    if not camera_id:
        logging.warning("Received message without a camera_id.")
        return

    if camera_id not in camera_data:
        camera_data[camera_id] = {'violence_detected': False, 'chunks_buffer': deque(maxlen=10), 'chunks_since_violence': 0}
    
    state = camera_data[camera_id]
    chunk_info = {'timestamp': datetime.fromisoformat(body['timestamp']), 'object_name': body['object_name'], 'has_violence': body['has_violence']}
    logging.info(f"[{camera_id}] Received chunk {chunk_info['object_name']}. Violence: {chunk_info['has_violence']}")
    state['chunks_buffer'].append(chunk_info)

    # If a violence chunk is detected, reset the countdown and wait for post-violence chunks.
    if chunk_info['has_violence']:
        state['violence_detected'] = True
        state['chunks_since_violence'] = 0
        logging.info(f"[{camera_id}] Violence detected. Resetting/starting 2-chunk post-violence countdown.")
        return 

    # If we are in "violence detected" mode, check if the countdown is over.
    if state['violence_detected']:
        state['chunks_since_violence'] += 1
        
        # If we have received 2 non-violent chunks after the last violent one, process the video.
        if state['chunks_since_violence'] >= 2:
            logging.info(f"[{camera_id}] Post-violence countdown finished. Processing incident video.")
            
            violence_indices = [i for i, chunk in enumerate(state['chunks_buffer']) if chunk['has_violence']]
            
            if not violence_indices:
                logging.warning(f"[{camera_id}] Inconsistent state: violence_detected is True, but no violence chunks in buffer. Resetting.")
            else:
                first_violence_idx = violence_indices[0]
                last_violence_idx = violence_indices[-1]

                # Define window: 1 chunk before the first violence, and 2 chunks after the last one.
                start_idx = max(0, first_violence_idx - 1)
                end_idx = min(len(state['chunks_buffer']), last_violence_idx + 3)
                
                chunks_to_process = list(state['chunks_buffer'])[start_idx:end_idx]
                combine_chunks_and_upload(camera_id, chunks_to_process)

            # Reset state for the next event.
            state['violence_detected'] = False
            state['chunks_since_violence'] = 0
            state['chunks_buffer'].clear()

def rabbitmq_consumer_thread_func():
    logging.info("RabbitMQ consumer thread starting.")
    while True:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials, heartbeat=600))
            channel = connection.channel()
            exchange_name = 'ai_results_exchange'
            channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            binding_key = "ai.results.*" 
            channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=binding_key)
            logging.info(f"RabbitMQ consumer connected. Waiting for messages on binding key '{binding_key}'.")

            def callback(ch, method, properties, body):
                try:
                    process_message(json.loads(body))
                except Exception as e:
                    logging.error(f"Error in message callback: {e}")
                finally:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logging.error(f"RabbitMQ connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            logging.error(f"Unhandled exception in RabbitMQ thread: {e}")
            time.sleep(5)

# --- FastAPI Routes ---
@app.get("/", response_class=HTMLResponse)
async def get_ui():
    try:
        with open("templates/index.html") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="index.html not found")

@app.get("/api/data", response_class=JSONResponse)
async def api_data():
    with data_lock:
        return JSONResponse(content={
            'logs': list(system_logs),
            'events': list(detected_events)
        })

@app.get("/videos/{filename:path}")
async def serve_video(filename: str):
    video_path = os.path.join(LOCAL_VIDEO_DIR, filename)
    if not os.path.exists(video_path):
        return HTTPException(status_code=404, detail="Video not found")
    return FileResponse(video_path)

@app.get("/thumbnails/{filename:path}")
async def serve_thumbnail(filename: str):
    thumbnail_path = os.path.join(LOCAL_THUMBNAIL_DIR, filename)
    if not os.path.exists(thumbnail_path):
        return HTTPException(status_code=404, detail="Thumbnail not found")
    return FileResponse(thumbnail_path, media_type="image/jpeg")

# --- App Startup ---
@app.on_event("startup")
async def startup_event():
    consumer_thread = Thread(target=rabbitmq_consumer_thread_func, daemon=True)
    consumer_thread.start()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5003, reload=True) 