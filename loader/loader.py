import json
import os
import sys
import time
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Job, JobSkill 
from logger.logger import get_logger
from kafka_client import (
    create_kafka_producer,
    create_kafka_consumer,
    send_message,
    DATA_PROCESSING_TOPIC,
    JOB_STATUS_UPDATES_TOPIC,
    SYSTEM_NOTIFICATIONS_TOPIC,
    LOADER_CONSUMER_GROUP_ID
)

logger = get_logger("loader")

DATA_DIR = "/app/data"
DATABASE_URL = os.environ.get("DATABASE_URL")
PROGRESS_REPORT_INTERVAL_PERCENT = 10

def parse_date(date_str):
    """Parse date string into a datetime.date object."""
    if not date_str:
        return None
    try:
        if ' ' in date_str:
            date_str = date_str.split(' ')[0]
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        logger.warning(f"Could not parse date: {date_str}, setting to None.")
        return None


def load_transformed_data(file_path):
    """Load the transformed data from the JSON file."""
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
        logger.info(f"Successfully loaded {len(data)} records from {file_path}")
        return data
    except FileNotFoundError:
        logger.error(f"Data file not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {file_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Failed to load transformed data from {file_path}: {e}")
        return None


def process_job_listing(session, job_data):
    """Process a single job listing and add it to the database. Returns the new Job object or None."""
    if not all(k in job_data for k in ["title", "company", "location"]):
        logger.warning(f"Skipping job due to missing required fields: {job_data.get('title', 'N/A')} at {job_data.get('company', 'N/A')}")
        return None

    # Check if job already exists
    existing_job = session.query(Job).filter_by(
        title=job_data["title"],
        company_name=job_data["company"]
    ).first()

    if existing_job:
        logger.info(f"Job already exists, skipping: {job_data['title']} at {job_data['company']}")
        return None

    new_job = Job(
        title=job_data["title"],
        company_name=job_data["company"],
        location=job_data["location"],
        job_url=job_data.get("url"),
        date_posted=parse_date(job_data.get("date_posted")),
        date_scraped=datetime.now(),
        progress="Haven't Applied",
    )

    for skill in job_data.get("extracted_skills", []):
        if skill and skill.strip():
            new_job.skills.append(JobSkill(skill=skill.strip()))

    session.add(new_job)
    logger.debug(f"Prepared new job for addition: {new_job.title} at {new_job.company_name}")
    return new_job

# Kafka Message Handling
def send_status_update(producer, topic, job_id, event_type, **kwargs):
    """Helper to send status/notification messages via Kafka."""
    message = {
        "job_id": job_id,
        "event_type": event_type,
        "source": "loader",
        "timestamp": time.time()
    }
    message.update(kwargs)
    send_message(producer, topic, message)


def process_loading_request(producer, job_id):
    """Handles a loading request for a specific job ID."""
    logger.info(f"Processing loading request for job_id: {job_id}")

    file_path = os.path.join(DATA_DIR, f"{job_id}_jobs.json")

    job_listings = load_transformed_data(file_path)
    if job_listings is None:
        send_status_update(producer, SYSTEM_NOTIFICATIONS_TOPIC, job_id, "job_failed",
                           error_details=f"Failed to load or find data file: {file_path}")
        return

    total_jobs = len(job_listings)
    if total_jobs == 0:
        logger.warning(f"No job listings found in file {file_path} for job_id {job_id}. Considering complete.")
        send_status_update(producer, JOB_STATUS_UPDATES_TOPIC, job_id, "loading_complete")
        try:
            os.remove(file_path)
            logger.info(f"Deleted empty processed file: {file_path}")
        except OSError as e:
            logger.error(f"Error deleting empty file {file_path}: {e}")
        return

    if not DATABASE_URL:
        logger.error("DATABASE_URL environment variable not set")
        send_status_update(producer, SYSTEM_NOTIFICATIONS_TOPIC, job_id, "job_failed",
                           error_details="DATABASE_URL not configured in loader service")
        return

    engine = None
    session = None
    try:
        logger.info("Connecting to database...")
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        logger.info("Database connection successful.")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        send_status_update(producer, SYSTEM_NOTIFICATIONS_TOPIC, job_id, "job_failed",
                           error_details=f"Database connection error: {e}")
        return

    new_jobs_count = 0
    processed_count = 0
    last_reported_progress = -1

    try:
        for index, job_data in enumerate(job_listings):
            processed_count += 1
            new_job_object = process_job_listing(session, job_data)
            if new_job_object:
                new_jobs_count += 1

            current_progress = int(((index + 1) / total_jobs) * 100)
            if current_progress >= last_reported_progress + PROGRESS_REPORT_INTERVAL_PERCENT or current_progress == 100:
                 send_status_update(producer, JOB_STATUS_UPDATES_TOPIC, job_id, "loading_progress",
                                    percentage=float(current_progress))
                 last_reported_progress = current_progress

        logger.info(f"Committing {new_jobs_count} new jobs for job_id {job_id}...")
        session.commit()
        logger.info(f"Successfully loaded {new_jobs_count} new jobs (processed {processed_count}) for job_id {job_id}.")

        send_status_update(producer, JOB_STATUS_UPDATES_TOPIC, job_id, "loading_complete")

        try:
            os.remove(file_path)
            logger.info(f"Deleted processed file: {file_path}")
        except OSError as e:
            logger.error(f"Error deleting processed file {file_path}: {e}")
            send_status_update(producer, SYSTEM_NOTIFICATIONS_TOPIC, job_id, "system_warning",
                               details=f"Failed to delete processed file: {file_path}. Error: {e}")

    except Exception as e:
        logger.error(f"Error during data loading for job_id {job_id}: {e}")
        if session:
            session.rollback()
        send_status_update(producer, SYSTEM_NOTIFICATIONS_TOPIC, job_id, "job_failed",
                           error_details=f"Error processing/loading job listings: {e}")
    finally:
        if session:
            session.close()
            logger.debug(f"Database session closed for job_id {job_id}.")

def main_consumer_loop():
    """Main loop to consume messages from Kafka and trigger processing."""
    logger.info("Starting Loader Service...")
    producer = create_kafka_producer()
    consumer = create_kafka_consumer(DATA_PROCESSING_TOPIC, LOADER_CONSUMER_GROUP_ID)

    if not producer or not consumer:
        logger.error("Failed to initialize Kafka producer or consumer. Exiting.")
        sys.exit(1)

    logger.info(f"Listening for messages on topic '{DATA_PROCESSING_TOPIC}'...")
    try:
        for message in consumer:
            logger.debug(f"Received message: {message.topic} partition={message.partition} offset={message.offset} key={message.key}")
            try:
                msg_data = message.value
                event_type = msg_data.get("event_type")
                job_id = msg_data.get("job_id")
                source = msg_data.get("source") # Expected to be 'scraper'

                if event_type == "loading_requested" and job_id:
                    logger.info(f"Received '{event_type}' from '{source}' for job_id: {job_id}")
                    # Trigger the actual processing logic
                    process_loading_request(producer, job_id)
                else:
                    logger.warning(f"Received unexpected message or missing job_id: Type='{event_type}', JobID='{job_id}', Data='{msg_data}'")

            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON message: {message.value}")
            except Exception as e:
                logger.error(f"Error processing Kafka message: {e}", exc_info=True)
                job_id_for_error = msg_data.get("job_id", "unknown") if 'msg_data' in locals() else "unknown"
                send_status_update(producer, SYSTEM_NOTIFICATIONS_TOPIC, job_id_for_error, "job_failed",
                                   error_details=f"Internal error processing Kafka message: {e}")

    except KeyboardInterrupt:
        logger.info("Consumer loop interrupted by user (KeyboardInterrupt). Shutting down...")
    except Exception as e:
        logger.error(f"Critical error in consumer loop: {e}", exc_info=True)
    finally:
        logger.info("Closing Kafka consumer and producer...")
        if consumer:
            consumer.close()
        if producer:
            producer.close()
        logger.info("Loader service stopped.")

if __name__ == "__main__":
    if not DATABASE_URL:
        logger.error("FATAL: DATABASE_URL environment variable not set at startup.")
        sys.exit(1)
    main_consumer_loop()