import asyncio
import logging
import os
import re
import shutil
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Set

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from minio import Minio
from minio.error import S3Error
from pika import PlainCredentials, BlockingConnection, ConnectionParameters, BasicProperties
from pika.exceptions import AMQPConnectionError
from watchdog.events import FileSystemEventHandler, FileModifiedEvent, LoggingEventHandler, FileSystemEvent
from watchdog.observers.polling import PollingObserver

from models.chunk_metadata import ChunkMetadata

# Load environment variables from .env file if it exists
load_dotenv()

# --- Configuration ---
MINIO_URL = os.getenv("MINIO_URL", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "Noureldin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "Nour123#")
MINIO_BUCKET_VIDEO_CHUNKS = os.getenv("MINIO_BUCKET_VIDEO_CHUNKS", "video-chunks")
MINIO_USE_SECURE = os.getenv("MINIO_USE_SECURE", "False").lower() == "true"

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "Noureldin")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "Nour123#")

# Ensure SRS_BASE_URL does not have a trailing slash for robust joining
SRS_BASE_URL = os.getenv("SRS_BASE_URL", "http://srs:8080/live").rstrip('/')
FFMPEG_SEGMENT_DURATION = int(os.getenv("FFMPEG_SEGMENT_DURATION", "5"))  # seconds
OUTPUT_DIR_BASE = Path(os.getenv("OUTPUT_DIR_BASE", "/tmp/hls_output"))

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(process)d - %(threadName)s - %(message)s')
logger = logging.getLogger(__name__)

# --- FastAPI App ---
app = FastAPI(
    title="AI Pipeline Service",
    description="Manages video stream chunking for AI processing."
)

# --- Global State & Clients ---
active_processes: Dict[str, asyncio.subprocess.Process] = {}
active_observers: Dict[str, PollingObserver] = {}
# Store stdout/stderr reading tasks per camera_id
active_tasks: Dict[str, Dict[str, asyncio.Task]] = {} 
# Store processed .ts files per camera_id to avoid duplicates
processed_ts_files: Dict[str, Set[str]] = {}
minio_client: Optional[Minio] = None


# --- Helper Functions & Classes ---

def get_rabbitmq_connection():
    credentials = PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    try:
        connection = BlockingConnection(
            ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials, heartbeat=600, blocked_connection_timeout=300)
        )
        return connection
    except AMQPConnectionError as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        return None

def publish_to_rabbitmq(camera_id: str, metadata: ChunkMetadata):
    connection = get_rabbitmq_connection()
    if not connection:
        logger.error(f"[{camera_id}] Could not publish metadata, RabbitMQ connection failed.")
        return

    try:
        channel = connection.channel()
        exchange_name = 'ai_camera_exchange'
        routing_key = f"ai.camera.{camera_id}"

        # Declare a topic exchange, durable so it survives broker restarts
        channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)
        logger.info(f"[{camera_id}] Ensured RabbitMQ exchange '{exchange_name}' of type 'topic' exists.")

        channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=metadata.to_json(),
            properties=BasicProperties(
                delivery_mode=2,  # make message persistent
                content_type='application/json'
            )
        )
        logger.info(f"[{camera_id}] Successfully published metadata to exchange '{exchange_name}' with routing key '{routing_key}': {metadata.chunk_url}")
    except Exception as e:
        logger.error(f"[{camera_id}] Error publishing to RabbitMQ: {e}")
    finally:
        if connection and connection.is_open:
            connection.close()

class M3U8ModifiedHandler(FileSystemEventHandler):
    def __init__(self, camera_id: str, stream_output_dir: Path, m3u8_filename: str):
        self.camera_id = camera_id
        self.stream_output_dir = stream_output_dir
        self.m3u8_file_path = stream_output_dir / m3u8_filename
        self.minio_client = minio_client
        if self.camera_id not in processed_ts_files:
            processed_ts_files[self.camera_id] = set()
        logger.info(f"[{camera_id}] M3U8ModifiedHandler initialized for {self.m3u8_file_path}. Watching for creations/modifications of this M3U8 file.")

    def _handle_m3u8_event(self, event_src_path: str, event_type_for_log: str):
        if event_src_path == str(self.m3u8_file_path):
            logger.info(f"[{self.camera_id}] M3U8 file '{event_type_for_log}' event MATCHED target for processing: {event_src_path}")
            try:
                if not self.m3u8_file_path.exists():
                    logger.warning(f"[{self.camera_id}] M3U8 file {self.m3u8_file_path} does not exist at time of processing. Skipping.")
                    return

                with open(self.m3u8_file_path, 'r') as f:
                    content = f.read()
                
                current_ts_files_in_playlist = set(re.findall(r"^(?!#)(.*\.ts)$", content, re.MULTILINE))
                
                if not current_ts_files_in_playlist:
                    logger.info(f"[{self.camera_id}] No .ts files found in M3U8 playlist {self.m3u8_file_path} content at this time.")
                    return

                logger.info(f"[{self.camera_id}] Found .ts files in M3U8: {current_ts_files_in_playlist}")
                
                session_processed_files = processed_ts_files.get(self.camera_id, set())
                
                new_ts_files_to_process = current_ts_files_in_playlist - session_processed_files
                
                if not new_ts_files_to_process:
                    logger.info(f"[{self.camera_id}] No new .ts files to process from M3U8.")
                    return

                logger.info(f"[{self.camera_id}] New .ts files to process: {new_ts_files_to_process}")

                for ts_filename in new_ts_files_to_process:
                    chunk_path = self.stream_output_dir / ts_filename
                    
                    if not chunk_path.exists():
                        logger.warning(f"[{self.camera_id}] .ts file {ts_filename} listed in M3U8 but not found at {chunk_path}. Skipping.")
                        continue

                    logger.info(f"[{self.camera_id}] Processing new HLS segment from M3U8: {ts_filename} at {chunk_path}")

                    if not self.minio_client:
                        logger.error(f"[{self.camera_id}] MinIO client not initialized. Cannot upload {ts_filename}.")
                        continue

                    minio_object_name = f"{self.camera_id}/{ts_filename}"
                    try:
                        logger.info(f"[{self.camera_id}] Attempting to upload {str(chunk_path)} to MinIO bucket '{MINIO_BUCKET_VIDEO_CHUNKS}' as '{minio_object_name}'")
                        self.minio_client.fput_object(
                            MINIO_BUCKET_VIDEO_CHUNKS,
                            minio_object_name,
                            str(chunk_path),
                            content_type='video/MP2T'
                        )
                        logger.info(f"[{self.camera_id}] Successfully uploaded {ts_filename} to MinIO.")

                        minio_host_for_url = MINIO_URL.split(':')[0]
                        minio_access_url = f"http://{minio_host_for_url}:9000/{MINIO_BUCKET_VIDEO_CHUNKS}/{minio_object_name}"

                        metadata = ChunkMetadata(
                            camera_id=self.camera_id,
                            chunk_url=minio_access_url,
                            timestamp=datetime.utcnow().isoformat(),
                            duration_seconds=FFMPEG_SEGMENT_DURATION
                        )
                        publish_to_rabbitmq(self.camera_id, metadata)
                        
                        session_processed_files.add(ts_filename)

                        try:
                            chunk_path.unlink()
                            logger.info(f"[{self.camera_id}] Deleted local segment {ts_filename}")
                        except OSError as e:
                            logger.error(f"[{self.camera_id}] Error deleting local segment {ts_filename}: {e}")

                    except S3Error as e:
                        logger.error(f"[{self.camera_id}] MinIO S3 Error uploading {ts_filename}: {e}")
                    except Exception as e:
                        logger.error(f"[{self.camera_id}] Unexpected error processing chunk {ts_filename}: {e}", exc_info=True)
                
                processed_ts_files[self.camera_id] = session_processed_files

            except FileNotFoundError:
                 logger.warning(f"[{self.camera_id}] M3U8 file {self.m3u8_file_path} not found during {event_type_for_log} handler. FFmpeg might not have created it yet or it was deleted.")
            except Exception as e:
                logger.error(f"[{self.camera_id}] Error in M3U8ModifiedHandler ({event_type_for_log} event): {e}", exc_info=True)
        # else: # Optional: logging for ignored events, can be verbose
            # logger.debug(f"[{self.camera_id}] M3U8 Handler: Ignored {event_type_for_log} event for {event_src_path} (doesn't match target M3U8 or is a directory).")

    def on_any_event(self, event): # event is FileSystemEvent or one of its subclasses
        logger.info(f"[{self.camera_id}] Watchdog ON_ANY_EVENT: Type='{event.event_type}', Path='{event.src_path}', IsDir={event.is_directory}")

    def on_created(self, event: FileSystemEvent):
        logger.info(f"[{self.camera_id}] Watchdog ON_CREATED specific handler TRIGGERED for: Path='{event.src_path}', IsDir={event.is_directory}")
        if not event.is_directory:
             self._handle_m3u8_event(event.src_path, "created")

    def on_modified(self, event: FileSystemEvent): 
        logger.info(f"[{self.camera_id}] Watchdog ON_MODIFIED specific handler TRIGGERED for: Path='{event.src_path}', IsDir={event.is_directory}")
        if not event.is_directory:
            self._handle_m3u8_event(event.src_path, "modified")


# --- FastAPI Endpoints ---
@app.on_event("startup")
def startup_event():
    global minio_client
    logger.info("Application startup event triggered.")
    try:
        logger.info(f"Attempting to initialize MinIO client with URL: {MINIO_URL}, AccessKey: {MINIO_ACCESS_KEY is not None}")
        minio_client = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_USE_SECURE
        )
        if not minio_client.bucket_exists(MINIO_BUCKET_VIDEO_CHUNKS):
            minio_client.make_bucket(MINIO_BUCKET_VIDEO_CHUNKS)
            logger.info(f"MinIO bucket '{MINIO_BUCKET_VIDEO_CHUNKS}' created.")
        else:
            logger.info(f"MinIO bucket '{MINIO_BUCKET_VIDEO_CHUNKS}' already exists.")
        logger.info("MinIO client initialized successfully.")
    except Exception as e:
        logger.error(f"CRITICAL: Failed to initialize MinIO client: {e}", exc_info=True)
        minio_client = None

    OUTPUT_DIR_BASE.mkdir(parents=True, exist_ok=True)
    logger.info(f"Base HLS output directory ensured: {OUTPUT_DIR_BASE}")

async def read_stream(stream, camera_id_guid, stream_name):
    while True:
        line = await stream.readline()
        if line:
            logger.info(f"[{camera_id_guid}] FFmpeg {stream_name}: {line.decode(errors='ignore').strip()}")
        else:
            break

@app.post("/process/start/{camera_id_guid}")
async def start_processing(camera_id_guid: str):
    logger.info(f"Received start processing request for camera_id_guid: {camera_id_guid}")
    if not minio_client:
        logger.error("MinIO client not available. Cannot start processing.")
        raise HTTPException(status_code=503, detail="MinIO service not available. Cannot start processing.")

    if not camera_id_guid:
        raise HTTPException(status_code=400, detail="camera_id_guid cannot be empty.")

    if camera_id_guid in active_processes:
        logger.warning(f"[{camera_id_guid}] Processing is already active.")
        return {"message": "Processing is already active for this camera_id"}

    srs_stream_name_part = f"camera-{camera_id_guid}-user-1"
    stream_url = f"{SRS_BASE_URL}/{srs_stream_name_part}.flv"

    logger.info(f"Target SRS Stream Name Part: {srs_stream_name_part}")
    logger.info(f"Using SRS_BASE_URL: {SRS_BASE_URL}")
    logger.info(f"[{camera_id_guid}] Constructed final FFmpeg input stream_url: {stream_url}")
    logger.info(f"[{camera_id_guid}] Preparing FFmpeg to PULL from the above URL as a CLIENT.")

    stream_output_dir = OUTPUT_DIR_BASE / camera_id_guid
    stream_output_dir.mkdir(parents=True, exist_ok=True)
    m3u8_filename = f"{camera_id_guid}.m3u8"
    m3u8_filepath = stream_output_dir / m3u8_filename
    # Use camera_id_guid for unique .ts filenames within the session's HLS output directory
    # FFmpeg will append the timestamp due to -strftime 1
    ts_segment_filename_template = f"{camera_id_guid}-%Y%m%d%H%M%S.ts" 

    ffmpeg_cmd = [
        "ffmpeg",
        "-loglevel", "debug", # Increased verbosity for FFmpeg
        "-i", stream_url,
        "-c:v", "copy",
        "-c:a", "aac",        # Ensure AAC audio for broader compatibility
        "-async", "1",         # Audio sync method
        "-flags", "+global_header", # Needed for some players/muxers
        "-f", "hls",
        "-hls_time", str(FFMPEG_SEGMENT_DURATION), # Duration of each segment
        "-hls_list_size", "2",  # Keep a rolling list of 2 segments in M3U8 (adjust as needed)
        "-hls_flags", "omit_endlist", # Continuously update M3U8, don't mark as finished
        "-strftime", "1",      # Enable strftime in segment filenames for unique names
        "-hls_segment_filename", str(stream_output_dir / ts_segment_filename_template),
        str(m3u8_filepath)
    ]

    logger.info(f"[{camera_id_guid}] Starting FFmpeg process with command: {' '.join(ffmpeg_cmd)}")
    try:
        process = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        active_processes[camera_id_guid] = process
        logger.info(f"[{camera_id_guid}] FFmpeg process started (PID: {process.pid}).")

        # Initialize the set for processed files for this new session
        if camera_id_guid not in processed_ts_files:
            processed_ts_files[camera_id_guid] = set()
            logger.info(f"[{camera_id_guid}] Initialized empty set for processed .ts files.")
        else:
            # If restarting a session for some reason, clear old processed files for this ID
            processed_ts_files[camera_id_guid].clear()
            logger.info(f"[{camera_id_guid}] Cleared existing set for processed .ts files for new session.")

        # Start tasks to read stdout and stderr
        stdout_task = asyncio.create_task(read_stream(process.stdout, camera_id_guid, "stdout"))
        stderr_task = asyncio.create_task(read_stream(process.stderr, camera_id_guid, "stderr"))
        active_tasks[camera_id_guid] = {"stdout": stdout_task, "stderr": stderr_task}

        # Initialize and start Watchdog observer
        # event_handler = LoggingEventHandler(logger=logger) # For basic logging
        event_handler = M3U8ModifiedHandler(camera_id_guid, stream_output_dir, m3u8_filename)
        observer = PollingObserver() # Using PollingObserver as InotifyObserver might have issues in Docker tmpfs
        observer.schedule(event_handler, str(stream_output_dir), recursive=False)
        observer.start()
        active_observers[camera_id_guid] = observer
        logger.info(f"[{camera_id_guid}] Successfully started FFmpeg (PID: {process.pid}) and Watchdog PollingObserver on directory {stream_output_dir}.")

        return {"message": f"Successfully started processing for {camera_id_guid}", "ffmpeg_pid": process.pid}

    except Exception as e:
        logger.error(f"[{camera_id_guid}] Failed to start FFmpeg process or Watchdog: {e}", exc_info=True)
        if process and camera_id_guid in active_processes:
            active_processes.pop(camera_id_guid, None)
            if process.returncode is None:
                process.kill()
        processed_ts_files.pop(camera_id_guid, None)
        if stream_output_dir.exists():
            shutil.rmtree(stream_output_dir)
            logger.info(f"[{camera_id_guid}] Cleaned up HLS output directory due to startup error: {stream_output_dir}")
        raise HTTPException(status_code=500, detail=f"Failed to start processing for {camera_id_guid}: {str(e)}")

@app.post("/process/stop/{camera_id_guid}")
async def stop_processing(camera_id_guid: str):
    logger.info(f"Received stop processing request for camera_id_guid: {camera_id_guid}")

    # Clear the set of processed files for this session
    if camera_id_guid in processed_ts_files:
        processed_ts_files[camera_id_guid].clear()
        logger.info(f"[{camera_id_guid}] Cleared processed .ts files set for session.")

    process = active_processes.pop(camera_id_guid, None)
    tasks = active_tasks.pop(camera_id_guid, None)
    observer = active_observers.pop(camera_id_guid, None)

    if process:
        logger.info(f"[{camera_id_guid}] Terminating FFmpeg process (PID: {process.pid})...")
        try:
            if process.returncode is None: # Check if process is still running
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=10.0) # Wait for termination
            logger.info(f"[{camera_id_guid}] FFmpeg process terminated with code: {process.returncode}.")
        except asyncio.TimeoutError:
            logger.warning(f"[{camera_id_guid}] Timeout waiting for FFmpeg to terminate. Killing...")
            if process.returncode is None:
                 process.kill()
                 await process.wait() # Ensure kill completes
            logger.info(f"[{camera_id_guid}] FFmpeg process killed.")
        except Exception as e:
            logger.error(f"[{camera_id_guid}] Exception during FFmpeg process termination: {e}", exc_info=True)
    else:
        logger.warning(f"[{camera_id_guid}] No active FFmpeg process found to stop.")

    if tasks:
        stdout_task = tasks.get("stdout")
        stderr_task = tasks.get("stderr")
        try:
            if stdout_task and not stdout_task.done():
                stdout_task.cancel()
                try:
                    await stdout_task
                except asyncio.CancelledError:
                    logger.info(f"[{camera_id_guid}] FFmpeg stdout reading task cancelled.")
            if stderr_task and not stderr_task.done():
                stderr_task.cancel()
                try:
                    await stderr_task
                except asyncio.CancelledError:
                    logger.info(f"[{camera_id_guid}] FFmpeg stderr reading task cancelled.")
        except Exception as e:
            logger.error(f"[{camera_id_guid}] Error cancelling FFmpeg stream reading tasks: {e}", exc_info=True)

    if observer:
        logger.info(f"[{camera_id_guid}] Stopping Watchdog observer...")
        observer.stop()
        observer.join() # Wait for the observer thread to finish
        logger.info(f"[{camera_id_guid}] Watchdog observer stopped.")
    else:
        logger.warning(f"[{camera_id_guid}] No active Watchdog observer found to stop.")

    # Clean up HLS output directory
    stream_output_dir = OUTPUT_DIR_BASE / camera_id_guid
    if stream_output_dir.exists():
        try:
            shutil.rmtree(stream_output_dir)
            logger.info(f"[{camera_id_guid}] Cleaned up HLS output directory: {stream_output_dir}")
        except Exception as e:
            logger.error(f"[{camera_id_guid}] Error cleaning up HLS output directory {stream_output_dir}: {e}")

    return {"message": f"Successfully stopped processing for {camera_id_guid}"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "active_processes": list(active_processes.keys()), "active_observers": list(active_observers.keys())}

# To run this app (for local testing, Dockerfile will handle it in container):
# uvicorn main:app --host 0.0.0.0 --port 8000 --reload

if __name__ == "__main__":
    port = int(os.getenv("AI_PIPELINE_PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True) 