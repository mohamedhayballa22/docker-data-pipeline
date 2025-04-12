import json
import os
import threading
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
from logger.logger import get_logger
import uuid
import asyncio

logger = get_logger("api_kafka_client")

KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "kafka:9092")
REQUEST_TIMEOUT_MS = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "10000"))
RETRY_BACKOFF_MS = int(os.getenv("KAFKA_RETRY_BACKOFF_MS", "500"))
MAX_RETRIES = int(os.getenv("KAFKA_PRODUCER_RETRIES", "5"))
SCRAPING_JOBS_TOPIC = "scraping-jobs"
JOB_STATUS_UPDATES_TOPIC = "job-status-updates"
SYSTEM_NOTIFICATIONS_TOPIC = "system-notifications"
CONSUMER_TOPICS = [
    JOB_STATUS_UPDATES_TOPIC,
    SYSTEM_NOTIFICATIONS_TOPIC,
]
API_CONSUMER_GROUP_ID = "api_status_listener_group"

# Global Client Instances
producer = None
consumer = None
consumer_thread = None
stop_consumer_event = threading.Event()


# Producer Functions
def create_kafka_producer():
    """Creates and returns a KafkaProducer instance."""
    global producer
    if producer is None:
        logger.info(f"Attempting to connect Kafka Producer to {KAFKA_BROKER_URL}...")
        retries = 5
        while retries > 0:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKER_URL,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=MAX_RETRIES,
                    request_timeout_ms=REQUEST_TIMEOUT_MS,
                    retry_backoff_ms=RETRY_BACKOFF_MS,
                )
                logger.info("Kafka Producer connected successfully.")
                return producer
            except NoBrokersAvailable:
                retries -= 1
                logger.warning(
                    f"Kafka brokers not available at {KAFKA_BROKER_URL}. Retrying in 5 seconds... ({retries} retries left)"
                )
                if retries == 0:
                    logger.error("Failed to connect Kafka Producer after multiple retries.")
                    raise
                time.sleep(5)
            except KafkaError as e:
                 logger.error(f"An unexpected Kafka error occurred during producer connection: {e}")
                 raise
    return producer

def send_kafka_message(topic, message):
    """Sends a message to the specified Kafka topic."""
    if producer is None:
        logger.error("Kafka Producer is not initialized. Cannot send message.")
        raise KafkaError("Producer not initialized")

    try:
        logger.debug(f"Sending message to topic '{topic}': {message}")
        future = producer.send(topic, value=message)
        record_metadata = future.get(timeout=REQUEST_TIMEOUT_MS / 1000) # Convert ms to s
        logger.info(f"Message sent successfully to topic '{topic}', partition {record_metadata.partition}, offset {record_metadata.offset}")
        return True
    except KafkaError as e:
        logger.error(f"Failed to send message to Kafka topic '{topic}': {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred while sending message to Kafka: {e}")
        return False

def close_kafka_producer():
    """Closes the Kafka producer."""
    global producer
    if producer:
        logger.info("Closing Kafka Producer...")
        producer.flush(timeout=5) # Allow time to flush
        producer.close(timeout=5)
        producer = None
        logger.info("Kafka Producer closed.")

# Consumer Functions
def create_kafka_consumer(job_statuses, lock):
    """Creates and returns a KafkaConsumer instance subscribed to status and notification topics."""
    global consumer
    if consumer is None:
        logger.info(f"Attempting to connect Kafka Consumer to {KAFKA_BROKER_URL}...")
        retries = 5
        while retries > 0:
            try:
                consumer = KafkaConsumer(
                    *CONSUMER_TOPICS,
                    bootstrap_servers=KAFKA_BROKER_URL,
                    group_id=API_CONSUMER_GROUP_ID,
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    auto_commit_interval_ms=5000,
                )
                logger.info(f"Kafka Consumer connected and subscribed to topics: {CONSUMER_TOPICS} with group_id: {API_CONSUMER_GROUP_ID}")
                return consumer
            except NoBrokersAvailable:
                retries -= 1
                logger.warning(
                    f"Kafka brokers not available at {KAFKA_BROKER_URL}. Retrying in 5 seconds... ({retries} retries left)"
                )
                if retries == 0:
                    logger.error("Failed to connect Kafka Consumer after multiple retries.")
                    raise
                time.sleep(5)
            except KafkaError as e:
                 logger.error(f"An unexpected Kafka error occurred during consumer connection: {e}")
                 raise
    return consumer

def consume_kafka_messages(consumer_instance, stop_event, job_statuses, lock, loop, broadcast_func):
    """Continuously consumes messages from status and notification topics
       and triggers broadcast for relevant updates."""
    logger.info("Kafka consumer thread started. Listening for status updates and notifications.")
    try:
        # Loop indefinitely using the consumer as an iterator (blocks until messages arrive)
        for record in consumer_instance:
            if stop_event.is_set():
                 logger.info("Stop event detected, exiting consumer loop.")
                 break

            topic = record.topic
            try:
                message = record.value
                logger.info(f"Processing message from topic '{topic}': {message}")

                job_id = message.get("job_id")
                event_type = message.get("event_type")
                source = message.get("source", "unknown")
                event_timestamp = message.get("timestamp", time.time())

                if not job_id:
                    logger.warning(f"Received message on topic {topic} without 'job_id': {message}")
                    continue

                # Prepare data for updating job_statuses
                update_data = {
                    'last_update': event_timestamp,
                    'event_timestamp': event_timestamp,
                    'last_event_type': event_type,
                    'source': source
                }
                status_changed = False

                # Handle messages based on Topic
                if topic == JOB_STATUS_UPDATES_TOPIC:
                    if event_type == "job_started":
                        update_data['status'] = "RUNNING"
                        update_data['stage'] = source.upper()
                        update_data['percentage'] = 0.0
                        status_changed = True
                    elif event_type == "job_progress":
                        update_data['status'] = "RUNNING"
                        update_data['stage'] = source.upper()
                        update_data['percentage'] = message.get("percentage", update_data.get('percentage', 0.0))
                        status_changed = True
                    elif event_type == "loading_progress":
                        update_data['status'] = "LOADING DATA"
                        update_data['stage'] = "LOADING DATA"
                        update_data['percentage'] = message.get("percentage", update_data.get('percentage', 0.0))
                        status_changed = True
                    elif event_type == "loading_complete":
                        update_data['status'] = "COMPLETE"
                        update_data['stage'] = "LOADING DATA"
                        update_data['percentage'] = 100.0
                        status_changed = True
                    else:
                        logger.warning(f"Received unhandled event_type '{event_type}' on topic {JOB_STATUS_UPDATES_TOPIC} for job {job_id}")

                elif topic == SYSTEM_NOTIFICATIONS_TOPIC:
                    if event_type == "job_failed":
                        update_data['status'] = "FAILED"
                        update_data['stage'] = source.upper()
                        update_data['error_details'] = message.get("error_details", "No details provided.")
                        status_changed = True
                    else:
                         logger.warning(f"Received unhandled event_type '{event_type}' on topic {SYSTEM_NOTIFICATIONS_TOPIC} for job {job_id}")

                # Update Job Status and Broadcast if Changed
                if status_changed:
                    updated_status_info = None
                    with lock:
                        if job_id not in job_statuses:
                            job_statuses[job_id] = {"job_id": job_id}

                        # Merge new update data into the existing status
                        job_statuses[job_id].update(update_data)
                        updated_status_info = job_statuses[job_id].copy() # Get latest state for broadcast

                        logger.debug(f"Updated status for job {job_id}: {updated_status_info}")

                    if updated_status_info and loop and broadcast_func:
                        broadcast_payload = {
                            "status": updated_status_info.get("status"),
                            "stage": updated_status_info.get("stage"),
                            "percentage": updated_status_info.get("percentage"),
                            "error_details": updated_status_info.get("error_details"),
                            "last_update": updated_status_info.get("last_update"),
                            "last_event_type": updated_status_info.get("last_event_type"),
                        }
                        broadcast_payload = {k: v for k, v in broadcast_payload.items() if v is not None}

                        ws_message = {
                            "type": "status_update",
                            "job_id": job_id,
                            "data": broadcast_payload
                        }
                        asyncio.run_coroutine_threadsafe(broadcast_func(ws_message), loop)
                        logger.debug(f"Scheduled broadcast for job {job_id} update: {ws_message}")

            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode Kafka message: {e}. Raw record value: {record.value}")
            except Exception as e:
                logger.error(f"Error processing message from Kafka topic '{topic}': {e}", exc_info=True)

    except Exception as e:
        logger.error(f"Critical error in Kafka consumer loop: {e}", exc_info=True)
    finally:
        logger.info("Kafka consumer thread finished.")

def start_consumer_thread(job_statuses, lock, loop, broadcast_func):
    """Starts the Kafka consumer in a background thread."""
    global consumer, consumer_thread, stop_consumer_event
    if consumer is None:
        try:
            consumer = create_kafka_consumer(job_statuses, lock)
        except Exception as e:
             logger.error(f"Failed during Kafka Consumer creation: {e}. Cannot start consumer thread.")
             return False # Prevent thread start if consumer creation fails

        if consumer is None:
            logger.error("Cannot start consumer thread: Consumer initialization failed.")
            return False

    if consumer_thread is None or not consumer_thread.is_alive():
        stop_consumer_event.clear()
        consumer_thread = threading.Thread(
            target=consume_kafka_messages,
            args=(consumer, stop_consumer_event, job_statuses, lock, loop, broadcast_func),
            daemon=True
        )
        consumer_thread.start()
        logger.info("Kafka consumer background thread initiated.")
        return True
    else:
        logger.warning("Consumer thread already running.")
        return True

def stop_consumer_thread():
    """Signals the consumer thread to stop, closes the consumer, and waits for the thread."""
    global consumer_thread, consumer, stop_consumer_event
    if consumer_thread and consumer_thread.is_alive():
        logger.info("Signaling Kafka consumer thread to stop...")
        stop_consumer_event.set()
        consumer_thread.join(timeout=10)
        if consumer_thread.is_alive():
            logger.warning("Kafka consumer thread did not stop gracefully within timeout.")
        else:
            logger.info("Kafka consumer thread stopped.")
        consumer_thread = None
    else:
        logger.info("Kafka consumer thread not running or already stopped.")

    close_kafka_consumer()


def close_kafka_consumer():
    """Closes the Kafka consumer."""
    global consumer
    if consumer:
        logger.info("Closing Kafka Consumer...")
        try:
            consumer.close()
            logger.info("Kafka Consumer closed.")
        except Exception as e:
             logger.error(f"Error closing Kafka consumer: {e}")
        finally:
            consumer = None


# Helper for generating Job ID
def generate_job_id():
    return str(uuid.uuid4())