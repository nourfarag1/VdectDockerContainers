from datetime import datetime, timedelta
import os
import pika
import json
import time
import sys
import threading
import subprocess
import re
import shutil
from pathlib import Path
from urllib.parse import urlparse
from minio import Minio
from dotenv import load_dotenv

# Load environment variables from .env file for local development
load_dotenv()

# --- Configuration & Environment Variables ---
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASSWORD', 'guest')
MINIO_URL = os.getenv('MINIO_URL', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_USE_SECURE = os.getenv('MINIO_USE_SECURE', 'False').lower() == 'true'
EVENT_TIMEOUT_SECONDS = int(os.getenv('EVENT_TIMEOUT_SECONDS', 30)) # Time to wait for next chunk before finalizing
CAMERA_ID_TO_PROCESS = os.getenv('CAMERA_ID_TO_PROCESS', 'default_camera_id') # The specific camera this instance handles
SOURCE_BUCKET_NAME = os.getenv('SOURCE_BUCKET_NAME', 'video-chunks')
EVENTS_BUCKET_NAME = os.getenv('EVENTS_BUCKET_NAME', 'violence-events')

# --- State Management ---
# In-memory dictionary to track active violence events, as per our plan.
# Key: camera_id
# Value: a dictionary holding event details.
# e.g., { "first_violent_chunk": {}, "last_violent_chunk": {}, "chunks_in_event": [], "last_activity_timestamp": time.time() }
active_events = {}

# --- Client Initialization ---
try:
    minio_client = Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_USE_SECURE
    )
    print("Successfully connected to MinIO.")
except Exception as e:
    print(f"Error connecting to MinIO: {e}", file=sys.stderr)
    minio_client = None

# --- Helper Functions ---
def get_context_chunk(chunk_metadata, offset_seconds=-5):
    """
    Infers the object name for a preceding (N-1) or succeeding (L+1) chunk
    by adjusting the timestamp in the filename. It verifies the chunk exists in MinIO.
    """
    object_name = chunk_metadata.get('object_name')
    if not object_name:
        return None

    # Corrected Regex to find the timestamp in the filename (works with full path)
    match = re.search(r'-(\d{14})\.ts', object_name)
    if not match:
        print(f"Could not parse timestamp from chunk name: {object_name}")
        return None

    timestamp_str = match.group(1)
    base_name = object_name[:match.start()]

    try:
        current_dt = datetime.strptime(timestamp_str, '%Y%m%d%H%M%S')
        context_dt = current_dt + timedelta(seconds=offset_seconds)
        context_timestamp_str = context_dt.strftime('%Y%m%d%H%M%S')

        # This is a guess. The actual filename might not match this guess perfectly.
        new_object_name = f"{base_name}-{context_timestamp_str}.ts"

        # Crucial Step: Verify the inferred chunk actually exists before using it.
        try:
            minio_client.stat_object(SOURCE_BUCKET_NAME, new_object_name)
            print(f"Verified context chunk exists in MinIO: {new_object_name}")
        except Exception:
            print(f"Inferred context chunk '{new_object_name}' does not exist in MinIO. Skipping.")
            return None

        # Return a flat message structure consistent with the one from DummyAiWorker
        return {
            "object_name": new_object_name,
            "ai_result": "context",
            "camera_id": chunk_metadata.get("camera_id"),
            "minio_path": f"http://{MINIO_URL}/{SOURCE_BUCKET_NAME}/{new_object_name}",
            "source": "inferred"
        }

    except Exception as e:
        print(f"Error during context chunk inference: {e}", file=sys.stderr)
        return None

def finalize_event(camera_id, event_data):
    """
    Finalizes a violence event: downloads chunks, merges them with ffmpeg,
    uploads the result, and cleans up.
    """
    print(f"--- FINALIZING EVENT for camera {camera_id} ---")
    
    # Ensure the destination bucket exists
    try:
        if not minio_client.bucket_exists(EVENTS_BUCKET_NAME):
            minio_client.make_bucket(EVENTS_BUCKET_NAME)
            print(f"Created MinIO bucket: {EVENTS_BUCKET_NAME}")
    except Exception as e:
        print(f"Error checking/creating bucket {EVENTS_BUCKET_NAME}: {e}", file=sys.stderr)
        return

    # Create a temporary local directory to store chunks for merging
    local_temp_dir = Path(f"/tmp/event_{camera_id}_{int(time.time())}")
    local_temp_dir.mkdir(parents=True, exist_ok=True)
    
    downloaded_chunk_paths = []
    original_chunks_to_delete = []

    for chunk_message in event_data['chunks_in_event']:
        # CORRECTED: Get object_name directly from the message
        object_name = chunk_message.get('object_name')
        if not object_name:
            print(f"Skipping chunk with no object_name in message: {chunk_message}")
            continue

        local_path = local_temp_dir / Path(object_name).name
        
        try:
            print(f"Downloading {object_name} to {local_path}...")
            minio_client.fget_object(SOURCE_BUCKET_NAME, object_name, str(local_path))
            downloaded_chunk_paths.append(str(local_path))
            original_chunks_to_delete.append(object_name)
        except Exception as e:
            print(f"Failed to download chunk {object_name}: {e}", file=sys.stderr)
            # If a chunk fails to download, we can't create a complete event video.
            # We should stop and avoid deleting any chunks.
            return

    if not downloaded_chunk_paths:
        print("No chunks were downloaded. Aborting event finalization.")
        return

    # Create the file list for ffmpeg
    filelist_path = local_temp_dir / "filelist.txt"
    with open(filelist_path, 'w') as f:
        for path in downloaded_chunk_paths:
            f.write(f"file '{path}'\n")

    # Merge with ffmpeg
    merged_filename = f"event_{camera_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.mp4"
    merged_filepath = local_temp_dir / merged_filename
    ffmpeg_command = [
        'ffmpeg',
        '-f', 'concat',
        '-safe', '0',
        '-i', str(filelist_path),
        '-c', 'copy',
        str(merged_filepath)
    ]

    try:
        print(f"Running ffmpeg to merge {len(downloaded_chunk_paths)} chunks...")
        subprocess.run(ffmpeg_command, check=True, capture_output=True, text=True)
        print(f"Successfully merged video to {merged_filepath}")

        # Upload the merged video to MinIO
        minio_client.fput_object(
            EVENTS_BUCKET_NAME,
            merged_filename,
            str(merged_filepath),
            content_type='video/mp4'
        )
        print(f"Successfully uploaded {merged_filename} to bucket {EVENTS_BUCKET_NAME}")

        # --- Cleanup ---
        # print("Cleaning up original chunks...")
        # for object_name in original_chunks_to_delete:
        #     try:
        #         minio_client.remove_object(SOURCE_BUCKET_NAME, object_name)
        #         print(f"  -> Deleted {object_name}")
        #     except Exception as e:
        #         print(f"  -> Failed to delete {object_name}: {e}", file=sys.stderr)

    except subprocess.CalledProcessError as e:
        print(f"ffmpeg failed with exit code {e.returncode}", file=sys.stderr)
        print(f"ffmpeg stderr: {e.stderr}", file=sys.stderr)
    except Exception as e:
        print(f"An error occurred during ffmpeg processing or upload: {e}", file=sys.stderr)
    finally:
        # Clean up local temporary files robustly
        try:
            if local_temp_dir.exists():
                shutil.rmtree(local_temp_dir)
                print(f"Cleaned up local temporary directory: {local_temp_dir}")
        except Exception as e:
            print(f"Error during temporary directory cleanup: {e}", file=sys.stderr)
    
    print(f"--- END OF EVENT for camera {camera_id} ---")

def connect_to_rabbitmq():
    """Establishes a connection to RabbitMQ, with retries."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
            )
            print("Successfully connected to RabbitMQ.")
            return connection
        except pika.exceptions.AMQPConnectionError:
            print("Could not connect to RabbitMQ, retrying in 5 seconds...")
            time.sleep(5)

def message_callback(ch, method, properties, body):
    """Callback function to process messages from the AI results queue."""
    try:
        message = json.loads(body)
        
        # Get camera_id from message body, as we listen to a specific queue now
        camera_id = message.get('camera_id')
        if not camera_id:
            print("Received message without a camera_id. Discarding.", file=sys.stderr)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        ai_result = message.get('ai_result')

        # --- FIX: Derive object_name from minio_path and treat message as the metadata ---
        minio_path = message.get('minio_path')
        if not minio_path:
            print("Received message without 'minio_path'. Discarding.", file=sys.stderr)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        
        try:
            # The object name is the path part of the URL, minus the leading bucket name.
            # e.g., /video-chunks/camera_....ts -> camera_....ts
            parsed_path = urlparse(minio_path).path
            object_name = '/'.join(parsed_path.strip('/').split('/')[1:])
            message['object_name'] = object_name
        except Exception as e:
            print(f"Could not parse object_name from minio_path '{minio_path}': {e}", file=sys.stderr)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Use the modified message itself as the chunk_metadata
        chunk_metadata = message

        print(f"Received '{ai_result}' for camera {camera_id}, chunk: {chunk_metadata.get('object_name')}")

        event = active_events.get(camera_id)

        if ai_result == 'violence':
            if not event:
                # Start of a new event. Try to get the preceding chunk (N-1).
                print(f"Starting new violence event for camera {camera_id}.")
                pre_event_chunk = get_context_chunk(chunk_metadata, offset_seconds=-5)
                
                initial_chunks = []
                if pre_event_chunk:
                    print(f"Found pre-event context chunk: {pre_event_chunk.get('object_name')}")
                    initial_chunks.append(pre_event_chunk)
                
                initial_chunks.append(message)

                active_events[camera_id] = {
                    'first_violent_chunk': chunk_metadata,
                    'last_violent_chunk': chunk_metadata,
                    'chunks_in_event': initial_chunks,
                    'last_activity_timestamp': time.time()
                }
            else:
                # Continuing an existing event
                print(f"Continuing violence event for camera {camera_id}.")
                event['chunks_in_event'].append(message)
                event['last_violent_chunk'] = chunk_metadata
                event['last_activity_timestamp'] = time.time()

        elif ai_result == 'no_violence':
            if event:
                # End of a violence event. This is our L+1 chunk.
                print(f"Received 'no_violence' signal, finalizing event for camera {camera_id}.")
                event['chunks_in_event'].append(message)
                finalize_event(camera_id, event)
                del active_events[camera_id]
            else:
                # Standalone 'no_violence' chunk, not part of an event.
                # TODO: Implement logic to delete this chunk from MinIO to save space.
                print(f"Received standalone 'no_violence' for camera {camera_id}. No active event.")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError as e:
        print(f"Error decoding message body: {e}", file=sys.stderr)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False) # Discard poison message
    except Exception as e:
        print(f"An unexpected error occurred in callback: {e}", file=sys.stderr)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def check_for_timeouts():
    """Periodically checks for and finalizes timed-out events."""
    while True:
        time.sleep(10) # Check every 10 seconds
        now = time.time()
        
        # Create a copy of items to avoid issues with modifying dict during iteration
        for camera_id, event in list(active_events.items()):
            if (now - event['last_activity_timestamp']) > EVENT_TIMEOUT_SECONDS:
                print(f"Event for camera {camera_id} has timed out. Finalizing.")
                # Try to infer the next chunk (L+1) after the last known violent one.
                post_event_chunk = get_context_chunk(event['last_violent_chunk'], offset_seconds=5)
                if post_event_chunk:
                    print(f"Found post-event context chunk on timeout: {post_event_chunk.get('object_name')}")
                    event['chunks_in_event'].append(post_event_chunk)

                finalize_event(camera_id, event)
                del active_events[camera_id]

def main():
    """Main function to set up RabbitMQ and start consuming messages."""
    if not minio_client:
        print("Cannot start service, MinIO client is not initialized.", file=sys.stderr)
        sys.exit(1)

    # Start the timeout checker in a background thread
    # The 'daemon=True' ensures the thread will exit when the main program exits.
    timeout_thread = threading.Thread(target=check_for_timeouts, daemon=True)
    timeout_thread.start()
    print("Event timeout checker started in background.")

    connection = connect_to_rabbitmq()
    channel = connection.channel()

    # This service consumes results directly from a queue dedicated to a specific camera.
    queue_name = f"ai.results.{CAMERA_ID_TO_PROCESS}"

    print(f"AiResultHandlerService instance is configured to listen on queue: '{queue_name}'")
    
    channel.queue_declare(queue=queue_name, durable=True)
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=message_callback)

    print(f"[*] AiResultHandlerService waiting for messages. To exit press CTRL+C")
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Interrupted. Closing connection...")
        connection.close()
    except Exception as e:
        print(f"A critical error occurred: {e}", file=sys.stderr)
        if connection.is_open:
            connection.close()

if __name__ == '__main__':
    main() 