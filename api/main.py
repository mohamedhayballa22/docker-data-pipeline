import os
import threading
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Dict, Any
from api.models import JobItem, get_db, Job
from logger.logger import get_logger
from api.kafka_client import (
    create_kafka_producer,
    send_kafka_message,
    close_kafka_producer,
    start_consumer_thread,
    stop_consumer_thread,
    close_kafka_consumer,
    generate_job_id,
    SCRAPING_JOBS_TOPIC,
    KAFKA_BROKER_URL
)
from kafka.errors import KafkaError
import asyncio
from api.websockets import ConnectionManager


logger = get_logger("api")

# In-memory storage for job statuses
job_statuses: Dict[str, Dict[str, Any]] = {}
job_status_lock = threading.Lock()

# WebSocket Connection Manager
manager = ConnectionManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup:
    logger.info("API starting up...")
    loop = asyncio.get_running_loop()
    manager.set_loop(loop)

    producer_ready = False
    consumer_ready = False

    try:
        # Initialize Producer
        create_kafka_producer()
        producer_ready = True
        logger.info("Kafka Producer connection established.")

        # Start Consumer Thread
        if start_consumer_thread(job_statuses, job_status_lock, loop, manager.broadcast):
            consumer_ready = True
            logger.info("Kafka Consumer thread started successfully.")
        else:
             logger.error("Failed to start Kafka consumer thread.")

    except KafkaError as e:
        logger.error(f"Kafka connection failed during startup: {e}")
        if producer_ready: close_kafka_producer()
        raise RuntimeError(f"Failed to establish essential Kafka connection: {e}") from e
    except Exception as e:
         logger.error(f"Unexpected error during API startup: {e}")
         if producer_ready: close_kafka_producer()
         raise

    yield

    # Shutdown:
    logger.info("API shutting down...")
    stop_consumer_thread()
    close_kafka_consumer()
    close_kafka_producer()
    logger.info("Kafka resources closed. API shutdown complete.")

app = FastAPI(title="Data Pipeline API", lifespan=lifespan)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health_check():
    kafka_status = "connected" if create_kafka_producer() is not None else "error"
    return {"status": "healthy", "kafka_connection": kafka_status, "kafka_broker": KAFKA_BROKER_URL}

@app.get("/data", response_model=List[JobItem])
def get_data(db: Session = Depends(get_db)):
    try:
        jobs = db.query(Job).all()
        return jobs
    except Exception as e:
        logger.error(f"Database query error: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to retrieve data from database."
        )

@app.post("/trigger-job-pipeline", status_code=202)
async def trigger_job_pipeline():
    """
    Generates a job ID and sends a 'job requested' event to Kafka.
    """
    logger.info("Received request to trigger job pipeline via Kafka.")

    job_id = generate_job_id()
    logger.info(f"Generated Job ID: {job_id}")

    scraping_params = {
        "GOOGLE_API_KEY": os.getenv("GOOGLE_API_KEY"),
        "JOB_TITLES": os.getenv("JOB_TITLES", "Data Scientist"),
        "LOCATION": os.getenv("LOCATION", "USA"),
        "TIME_FILTER": os.getenv("TIME_FILTER", "7d"),
        "NUM_PAGES": int(os.getenv("NUM_PAGES", "1")),
        "MAX_JOBS": int(os.getenv("MAX_JOBS", "50")),
        "OUTPUT_DIR": os.getenv("OUTPUT_DIR", "/app/data"),
        "OUTPUT_FILENAME": os.getenv("OUTPUT_FILENAME", f"jobs.json")
    }

    # Construct Kafka Message
    message = {
        "job_id": job_id,
        "event_type": "job_requested",
        "timestamp": time.time(),
        "parameters": scraping_params
    }

    # Send to Kafka
    try:
        success = send_kafka_message(SCRAPING_JOBS_TOPIC, message)
        if not success:
             raise HTTPException(status_code=503, detail="Failed to publish job request to Kafka.")
    except KafkaError as e:
        logger.error(f"Kafka error during job trigger for {job_id}: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable: Could not connect to Kafka.")
    except Exception as e:
        logger.error(f"Unexpected error during job trigger for {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error during job trigger.")

    with job_status_lock:
        job_statuses[job_id] = {
            "status": "requested",
            "requested_at": message["timestamp"],
            "last_update": message["timestamp"],
            "details": "Job request sent to Kafka."
        }
        logger.debug(f"Initialized status for job {job_id}: {job_statuses[job_id]}")


    logger.info(f"Job {job_id} successfully requested via Kafka topic '{SCRAPING_JOBS_TOPIC}'.")
    return {"message": "Job pipeline trigger request accepted.", "job_id": job_id}


@app.get("/jobs/{job_id}/status")
def get_job_status(job_id: str):
    """Gets the current status of a specific job from in-memory store."""
    logger.debug(f"Request received for status of job ID: {job_id}")
    with job_status_lock:
        status_info = job_statuses.get(job_id)

    if status_info:
        logger.debug(f"Returning status for job {job_id}: {status_info}")
        return status_info
    else:
        logger.warning(f"Status requested for unknown job ID: {job_id}")
        raise HTTPException(status_code=404, detail=f"Status for job ID '{job_id}' not found.")
    
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)

    try:
        with job_status_lock:
            current_statuses = job_statuses.copy()
        await manager.send_initial_state(websocket, current_statuses)

        while True:
            data = await websocket.receive_text()
            logger.debug(f"Received text from WebSocket {websocket.client}: {data}")

    except WebSocketDisconnect:
        logger.info(f"WebSocket client {websocket.client} disconnected.")
        manager.disconnect(websocket)
    except Exception as e:
        client_info = websocket.client or "Unknown Client"
        logger.error(f"Error in WebSocket connection {client_info}: {e}")
        manager.disconnect(websocket)
        try:
             await websocket.close(code=1011)
        except Exception:
             pass