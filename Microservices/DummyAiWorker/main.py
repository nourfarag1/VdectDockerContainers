import json
import logging
import os
import time
import pika
from pika.exceptions import AMQPConnectionError
from dotenv import load_dotenv
from chunk_metadata import ChunkMetadata
from minio import Minio
from minio.error import S3Error
from urllib.parse import urlparse
import requests
from typing import Optional, Dict, List, Any # Added Dict, List, Any
import threading # For running RabbitMQ consumer in background
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse # For serving HTML
from fastapi.staticfiles import StaticFiles # For serving static files like CSS/JS if needed later
from collections import deque # For fixed-size list of recent stats
import uvicorn # To run the app
import random # For dummy processing time and results
from datetime import datetime
import asyncio
import aiohttp # Added for RabbitMQ Management API calls
import numpy as np # New: For linear regression

# Load environment variables (though Docker Compose will set them)
load_dotenv()

# --- Configuration ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "Noureldin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "Nour123#")

# This is the single, shared queue this worker will pull from.
RABBITMQ_CONSUME_QUEUE_NAME = "ai_processing_queue"
# This is the exchange that the ai-pipeline publishes to.
RABBITMQ_CONSUME_EXCHANGE_NAME = "ai_camera_exchange"
# The binding key to get all camera messages.
RABBITMQ_CONSUME_BINDING_KEY = "ai.camera.*"

# New: Define the queue for publishing AI results - This part remains mostly the same conceptually
RABBITMQ_RESULTS_EXCHANGE_NAME = "ai_results_exchange" # Changed for clarity
RABBITMQ_RESULTS_ROUTING_KEY_TEMPLATE = "ai.results.{camera_id}" # Changed for clarity

RABBITMQ_MANAGEMENT_PORT = int(os.getenv("RABBITMQ_MANAGEMENT_PORT", 15672)) # New
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/") # New (ensure it's URL encoded if needed for API calls)

MINIO_URL = os.getenv("MINIO_URL", "minio:9000") # Endpoint WITH port, no http/https
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "Noureldin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "Nour123#")
MINIO_USE_SECURE = os.getenv("MINIO_USE_SECURE", "False").lower() == "true"

UI_PORT = int(os.getenv("UI_PORT", "8000"))
MAX_RECENT_STATS = int(os.getenv("MAX_RECENT_STATS", "100")) # Number of recent chunk stats to keep

VDECT_AI_MODEL_URL = os.getenv("VDECT_AI_MODEL_URL", "http://vdece-ai-model:5002") # New AI model service URL

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - DummyAiWorker - %(message)s')
logger = logging.getLogger(__name__)

# --- FastAPI App & Global State ---
app = FastAPI(title="Dummy AI Worker with Stats UI")

# Mount a directory for static files (like index.html, css, js later)
# Assuming 'static' directory is in the same place as main.py
# Ensure this path is correct relative to where main.py runs in the Docker container (/app/static)
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")
else:
    logger.warning("Static files directory ('static') not found. UI may not load correctly.")
    # Create it if it doesn't exist for robustness, though it should be part of the Docker image
    os.makedirs("static", exist_ok=True)
    app.mount("/static", StaticFiles(directory="static"), name="static")

minio_client: Optional[Minio] = None

# Global stats storage
# Using a deque for recent_chunk_stats to automatically manage fixed size
# Thread-safety for these simple structures might be okay for now due to GIL and infrequent updates
# but for more complex state or higher concurrency, locks would be needed.
global_stats: Dict[str, Any] = {
    "total_messages_processed": 0,
    "last_message_received_at": None,
    "minio_status": "Initializing...",
    "rabbitmq_status": "Initializing...",
    "recent_chunk_stats": deque(maxlen=MAX_RECENT_STATS), # Stores dicts for each chunk
    "current_simulated_processing_time_min_ms": 100, # Default min
    "current_simulated_processing_time_max_ms": 500  # Default max
}

# --- MinIO Client Initialization ---
def initialize_minio_client():
    global minio_client
    try:
        logger.info(f"Initializing MinIO client for endpoint: {MINIO_URL}")
        minio_client = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_USE_SECURE
        )
        minio_client.bucket_exists("nonexistentbucketjustfortestingconnection") # Test connection
        logger.info("MinIO client initialized successfully.")
        global_stats["minio_status"] = "Connected"
    except Exception as e:
        logger.error(f"Failed to initialize MinIO client: {e}", exc_info=True)
        minio_client = None
        global_stats["minio_status"] = f"Error: {e}"

# --- Helper function for linear regression ---
def calculate_linear_trend(x_data: List[float], y_data: List[float]):
    if len(x_data) < 2 or len(y_data) < 2:
        return 0.0, 0.0 # Not enough data for a trendline
    
    # Ensure x and y are numpy arrays
    x = np.array(x_data)
    y = np.array(y_data)
    
    # Calculate slope (m) and intercept (b) using least squares
    # y = mx + b
    A = np.vstack([x, np.ones(len(x))]).T
    m, b = np.linalg.lstsq(A, y, rcond=None)[0]
    return m, b

# --- RabbitMQ Consumer Logic ---
def on_message_received_callback(ch, method, properties, body):
    msg_received_time = time.time()
    logger.info(f"Received message from RabbitMQ. Routing key: {method.routing_key}")
    global_stats["last_message_received_at"] = datetime.utcnow().isoformat()

    try:
        data = json.loads(body.decode('utf-8'))
        metadata = ChunkMetadata(**data)
        original_publish_time_str = metadata.timestamp
        logger.info(f"[{metadata.camera_id}] Processing chunk: {metadata.chunk_url}, Published at (UTC): {original_publish_time_str}")

        if not minio_client:
            logger.error(f"[{metadata.camera_id}] MinIO client not available. Requeueing.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            time.sleep(1)
            return

        parsed_url = urlparse(metadata.chunk_url)
        bucket_name = parsed_url.path.split('/')[1]
        object_name = '/'.join(parsed_url.path.split('/')[2:])
        download_start_time = time.time()
        download_duration_ms = -1.0 # Changed to milliseconds
        
        # Define the container path to the shared volume where chunks will be stored
        CONTAINER_SHARED_VIDEO_PATH = "/shared_videos"
        # Define the host path where the AI model can access the shared volume
        HOST_SHARED_VIDEO_PATH = "N:/College/GraduationProject/Code/Development/shared_video_processing"

        # Construct the temporary chunk path within the container's shared volume
        temp_chunk_filename = object_name.split('/')[-1]
        temp_chunk_path_container = os.path.join(CONTAINER_SHARED_VIDEO_PATH, temp_chunk_filename)
        
        # Ensure the directory exists inside the container's shared volume
        os.makedirs(CONTAINER_SHARED_VIDEO_PATH, exist_ok=True)

        try:
            minio_client.fget_object(bucket_name, object_name, temp_chunk_path_container)
            download_end_time = time.time()
            download_duration_ms = (download_end_time - download_start_time) * 1000 # Convert to ms
            logger.info(f"[{metadata.camera_id}] Downloaded {object_name} to {temp_chunk_path_container} in {download_duration_ms:.2f}ms.") # Changed log to ms

            # --- Call the Vdect AI Model Service ---
            ai_result = {"predicted_name": "error", "confidence": 0.0} # Default to error
            ai_processing_start_time = time.time() # New
            try:
                logger.info(f"[{metadata.camera_id}] Sending video to Vdect AI model at {VDECT_AI_MODEL_URL}/predict for processing.")
                
                # Translate the container path to the host path for the AI model
                video_path_for_ai_model = os.path.join(HOST_SHARED_VIDEO_PATH, temp_chunk_filename)
                
                # Send the host path to the downloaded chunk to the AI model service
                response = requests.post(f"{VDECT_AI_MODEL_URL}/predict", json={"video_path": video_path_for_ai_model}, verify=False) # Added verify=False for self-signed certs or HTTP only
                response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
                ai_result = response.json()
                logger.info(f"[{metadata.camera_id}] Received AI result: {ai_result}")
            except requests.exceptions.RequestException as req_err:
                logger.error(f"[{metadata.camera_id}] Error calling Vdect AI model service: {req_err}", exc_info=True)
                # If AI service is down or errors, requeue the message
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                # Optionally, remove the downloaded file if it won't be retried soon
                try:
                    os.remove(temp_chunk_path_container) # Use container path for removal
                except OSError:
                    pass
                return
            except json.JSONDecodeError as json_err:
                logger.error(f"[{metadata.camera_id}] JSON decode error from AI model response: {json_err}", exc_info=True)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                try:
                    os.remove(temp_chunk_path_container) # Use container path for removal
                except OSError:
                    pass
                return
            finally:
                # Clean up the downloaded temporary chunk file after processing or error
                try:
                    os.remove(temp_chunk_path_container) # Use container path for removal
                except OSError:
                    logger.warning(f"[{metadata.camera_id}] Could not remove temp chunk {temp_chunk_path_container}")
            
            ai_processing_time_ms = (time.time() - ai_processing_start_time) * 1000 # New: Convert to ms

            # Process the result from the real AI model
            predicted_name = ai_result.get("predicted_name", "error")
            has_violence = (predicted_name == "violence")
            
            # The simulated processing time can now be replaced by the actual time taken by the AI model if needed,
            # or kept as a minimum for network latency etc.
            # simulated_processing_time_s = time.time() - download_end_time # This was the old calculation
            simulated_processing_time_ms = ai_processing_time_ms # Use the actual AI processing time in ms

            result_message = {
                "camera_id": metadata.camera_id,
                "object_name": object_name, # Pass object name
                "timestamp": datetime.utcnow().isoformat(),
                "has_violence": has_violence,
                "predicted_name": predicted_name, # Include the predicted name directly
                "confidence": ai_result.get("confidence") # Include confidence
            }
            
            try:
                # The exchange is declared once by the thread that creates the channel.
                exchange_name = RABBITMQ_RESULTS_EXCHANGE_NAME
                # The routing key is what allows the consumer to filter messages.
                # We'll use the pattern 'ai.results.<camera_id>'
                routing_key = RABBITMQ_RESULTS_ROUTING_KEY_TEMPLATE.format(camera_id=metadata.camera_id)

                ch.basic_publish(
                    exchange=exchange_name,
                    routing_key=routing_key,
                    body=json.dumps(result_message),
                    properties=pika.BasicProperties(
                        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                    )
                )
                logger.info(f"[{metadata.camera_id}] Published AI result to exchange '{exchange_name}' with key '{routing_key}': Violence={has_violence}")
            except Exception as pub_e:
                logger.error(f"[{metadata.camera_id}] Failed to publish AI result: {pub_e}", exc_info=True)
            # --- End of AI Result Simulation ---

            total_worker_time_ms = (time.time() - msg_received_time) * 1000 # Changed to milliseconds

            chunk_stat = {
                "received_at": msg_received_time,
                "camera_id": metadata.camera_id,
                "chunk_url": metadata.chunk_url,
                "published_at_source": original_publish_time_str,
                "download_duration_ms": round(download_duration_ms, 2), # Changed to ms
                "ai_processing_time_ms": round(ai_processing_time_ms, 2), # New: AI processing time in ms
                "total_worker_time_ms": round(total_worker_time_ms, 2), # Changed to ms
                "ai_simulation_result": predicted_name # Use the actual predicted name
            }
            global_stats["recent_chunk_stats"].append(chunk_stat)
            global_stats["total_messages_processed"] += 1
            
            logger.info(f"[{metadata.camera_id}] STATS: Download={chunk_stat['download_duration_ms']:.2f}ms, AI_Proc={chunk_stat['ai_processing_time_ms']:.2f}ms, TotalWorker={chunk_stat['total_worker_time_ms']:.2f}ms, AI_Result={chunk_stat['ai_simulation_result']}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except S3Error as s3_err:
            logger.error(f"[{metadata.camera_id}] MinIO S3 Error: {s3_err}. Requeueing.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return
        except Exception as e:
            logger.error(f"[{metadata.camera_id}] Download error: {e}. Requeueing.", exc_info=True)
            return

    except json.JSONDecodeError as e:
        logger.error(f"JSON Decode Error: {e}. Rejecting (no requeue).")
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        logger.error(f"Unexpected error in on_message_received: {e}. Requeueing.", exc_info=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def rabbitmq_consumer_thread_func():
    logger.info("RabbitMQ consumer thread starting.")
    connection = None
    while True:
        try:
            global_stats["rabbitmq_status"] = "Connecting..."
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials, heartbeat=600, blocked_connection_timeout=300)
            )
            channel = connection.channel()

            # Declare the exchange we will consume from (published by ai-pipeline)
            channel.exchange_declare(exchange=RABBITMQ_CONSUME_EXCHANGE_NAME, exchange_type='topic', durable=True)
            
            # Declare the durable queue that will receive the messages
            channel.queue_declare(queue=RABBITMQ_CONSUME_QUEUE_NAME, durable=True)

            # Bind the queue to the exchange with our wildcard routing key
            channel.queue_bind(exchange=RABBITMQ_CONSUME_EXCHANGE_NAME, queue=RABBITMQ_CONSUME_QUEUE_NAME, routing_key=RABBITMQ_CONSUME_BINDING_KEY)
            logger.info(f"Bound queue '{RABBITMQ_CONSUME_QUEUE_NAME}' to exchange '{RABBITMQ_CONSUME_EXCHANGE_NAME}' with key '{RABBITMQ_CONSUME_BINDING_KEY}'")

            # Declare the exchange we will publish results to
            channel.exchange_declare(exchange=RABBITMQ_RESULTS_EXCHANGE_NAME, exchange_type='topic', durable=True)
            logger.info(f"Declared exchange '{RABBITMQ_RESULTS_EXCHANGE_NAME}' for publishing results.")

            global_stats["rabbitmq_status"] = f"Connected, consuming from '{RABBITMQ_CONSUME_QUEUE_NAME}'"
            logger.info(f"Waiting for messages on queue '{RABBITMQ_CONSUME_QUEUE_NAME}'.")
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=RABBITMQ_CONSUME_QUEUE_NAME, on_message_callback=on_message_received_callback)
            channel.start_consuming()
        except AMQPConnectionError as e:
            logger.error(f"RabbitMQ connection error: {e}. Retrying in 10s...")
            global_stats["rabbitmq_status"] = f"Connection Error: {e}. Retrying..."
            if connection and connection.is_open:
                connection.close()
            time.sleep(10)
        except KeyboardInterrupt:
            # Should be caught by main thread ideally
            logger.info("RabbitMQ consumer thread received KeyboardInterrupt.")
            break
        except Exception as e:
            logger.error(f"Unexpected error in RabbitMQ consumer thread: {e}. Retrying in 10s...", exc_info=True)
            global_stats["rabbitmq_status"] = f"Error: {e}. Retrying..."
            if connection and connection.is_open:
                connection.close()
            time.sleep(10)
            if connection and connection.is_open:
                connection.close()
    logger.info("RabbitMQ consumer thread stopped.")
    global_stats["rabbitmq_status"] = "Stopped"

# --- FastAPI Event Handlers ---
@app.on_event("startup")
async def startup_event():
    logger.info("Application startup event.")
    initialize_minio_client()
    # Start the RabbitMQ consumer in a background thread
    consumer_thread = threading.Thread(target=rabbitmq_consumer_thread_func, daemon=True)
    consumer_thread.start()
    logger.info("RabbitMQ consumer thread initiated.")

# --- FastAPI Endpoints ---
@app.get("/", response_class=HTMLResponse)
async def get_ui():
    # Try to read index.html from the 'static' directory.
    # This path needs to be correct *inside the container*.
    # If WORKDIR is /app, then it looks for /app/static/index.html
    html_file_path = "static/index.html"
    try:
        with open(html_file_path, "r") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        logger.error(f"Could not find {html_file_path}. Make sure it's in the Docker image.")
        raise HTTPException(status_code=404, detail=f"{html_file_path} not found. UI cannot be served.")
    except Exception as e:
        logger.error(f"Error reading {html_file_path}: {e}")
        raise HTTPException(status_code=500, detail=f"Error serving UI: {e}")

@app.get("/stats")
async def get_stats():
    global global_stats
    
    # Check current RabbitMQ connection state for the status string
    # (This part relies on the background thread updating rabbitmq_status)
    current_rabbitmq_status = global_stats["rabbitmq_status"] 

    # Fetch real-time rates from RabbitMQ Management API
    rabbitmq_rates = {"publish_rate": 0.0, "ack_rate": 0.0}
    async with aiohttp.ClientSession() as session:
        rabbitmq_rates = await get_rabbitmq_management_stats(session)

    # Calculate linear trend for Total Worker Time
    total_worker_times = [s["total_worker_time_ms"] for s in global_stats["recent_chunk_stats"]]
    # Use index as x-axis for trendline. If no data, x_data will be empty.
    x_data_for_trend = list(range(len(total_worker_times))) 
    
    slope, intercept = calculate_linear_trend(x_data_for_trend, total_worker_times)

    stats_data = {
        "rabbitmq_status": current_rabbitmq_status,
        "minio_status": global_stats["minio_status"],
        "total_messages_processed": global_stats["total_messages_processed"],
        "last_message_received_at": global_stats["last_message_received_at"],
        "recent_chunk_stats": list(global_stats["recent_chunk_stats"]), # Convert deque to list for JSON serialization
        "max_recent_stats": MAX_RECENT_STATS,
        "rabbitmq_publish_rate": rabbitmq_rates["publish_rate"], # New
        "rabbitmq_ack_rate": rabbitmq_rates["ack_rate"], # New
        "total_processing_time_trend": { # New: Linear trend data
            "slope": round(slope, 4),
            "intercept": round(intercept, 4)
        }
    }
    return stats_data

# --- RabbitMQ Connection Management ---
async def get_rabbitmq_management_stats(session: aiohttp.ClientSession) -> Dict[str, float]:
    """
    Fetches publish and acknowledge rates from RabbitMQ Management API.
    """
    rates = {"publish_rate": 0.0, "ack_rate": 0.0}
    vhost = RABBITMQ_VHOST if RABBITMQ_VHOST != "/" else "%2F" # URL encode default vhost
    url = f"http://{RABBITMQ_HOST}:{RABBITMQ_MANAGEMENT_PORT}/api/queues/{vhost}/{RABBITMQ_CONSUME_QUEUE_NAME}"
    auth = aiohttp.BasicAuth(RABBITMQ_USER, RABBITMQ_PASS)

    try:
        async with session.get(url, auth=auth, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                publish_details = data.get("message_stats", {}).get("publish_details", {})
                rates["publish_rate"] = publish_details.get("rate", 0.0)
                
                # 'ack_details' for broker acknowledgements if using publisher confirms.
                # 'deliver_get_details' might be more relevant for consumer ack rate.
                # Let's use 'ack_details' if available, or try 'deliver_get_details' or 'deliver_details'
                # as a proxy for messages being taken by the consumer.
                # The most accurate would be ack_details if you have consumer acks enabled and tracked.
                # For simplicity, let's try to find ack_details first.
                message_stats = data.get("message_stats", {})
                ack_details = message_stats.get("ack_details", {}) # Broker acks for pub confirms
                if ack_details: # Check if ack_details exist and have a rate
                     rates["ack_rate"] = ack_details.get("rate", 0.0)
                else: # Fallback to deliver_get if ack_details is not informative for consumer acks
                    deliver_get_details = message_stats.get("deliver_get_details", {}) # Messages delivered to consumers
                    rates["ack_rate"] = deliver_get_details.get("rate", 0.0)
                
                # If still zero, it might be deliver_no_ack_details if consumer acks are not sent
                # or if the queue is idle.
                if rates["ack_rate"] == 0.0:
                    deliver_no_ack_details = message_stats.get("deliver_no_ack_details", {})
                    rates["ack_rate"] = deliver_no_ack_details.get("rate",0.0)


            else:
                logger.warning(f"Failed to fetch RabbitMQ management stats: {response.status} for queue {RABBITMQ_CONSUME_QUEUE_NAME}")
    except aiohttp.ClientError as e:
        logger.error(f"Error fetching RabbitMQ management stats: {e}")
    except asyncio.TimeoutError:
        logger.error(f"Timeout fetching RabbitMQ management stats for queue {RABBITMQ_CONSUME_QUEUE_NAME}")
    return rates

# --- Main Execution (for Uvicorn) ---
if __name__ == "__main__":
    logger.info(f"Starting Uvicorn server on host 0.0.0.0 port {UI_PORT}")
    uvicorn.run(app, host="0.0.0.0", port=UI_PORT) 