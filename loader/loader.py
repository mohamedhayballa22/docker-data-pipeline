import json
import os
import sys
import time
from datetime import datetime
from sqlalchemy import create_engine, exc as sqlalchemy_exc
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

def parse_date(date_str):
    """Parse date string into a datetime.date object."""
    if not date_str:
        return None
    try:
        if ' ' in date_str:
            date_str = date_str.split(' ')[0]
        if 'T' in date_str:
             date_str = date_str.split('T')[0]
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        logger.warning(f"Could not parse date: '{date_str}', setting to None.")
        return None


def load_transformed_data(file_path: str, job_id: str):
    """Load the transformed data from the JSON file."""
    logger.info(f"[Job {job_id}] Attempting to load data from {file_path}")
    try:
        with open(file_path, "r", encoding='utf-8') as file:
            data = json.load(file)
        if isinstance(data, list):
            logger.info(f"[Job {job_id}] Successfully loaded {len(data)} records from {file_path}")
            return data
        else:
            logger.error(f"[Job {job_id}] Data in {file_path} is not a list.")
            return None
    except FileNotFoundError:
        logger.error(f"[Job {job_id}] Data file not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"[Job {job_id}] Error decoding JSON from {file_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"[Job {job_id}] Failed to load transformed data from {file_path}: {e}")
        return None


def process_job_listing(session, job_data: dict, existing_job_keys: set):
    """
    Process a single job listing, check for duplicates using the provided set,
    and prepare it for addition. Returns the new Job object or None if skipped/duplicate.
    """
    title = job_data.get("title")
    company = job_data.get("company")
    if not title or not company:
        logger.warning(f"Skipping job due to missing title/company: {job_data}")
        return None

    job_key = (title.strip().lower(), company.strip().lower())

    if job_key in existing_job_keys:
        return None

    if not job_data.get("location"):
         logger.warning(f"Skipping job due to missing location: {title} at {company}")
         return None

    new_job = Job(
        title=title.strip(),
        company_name=company.strip(),
        location=job_data["location"].strip(),
        job_url=job_data.get("url"),
        date_posted=parse_date(job_data.get("date_posted")),
        date_scraped=datetime.now().date(),
        progress="Haven't Applied",
    )

    skills = job_data.get("extracted_skills", [])
    if isinstance(skills, list):
        unique_skills = set()
        for skill in skills:
            if skill and isinstance(skill, str) and skill.strip():
                stripped_skill = skill.strip()
                if stripped_skill not in unique_skills:
                     new_job.skills.append(JobSkill(skill=stripped_skill))
                     unique_skills.add(stripped_skill)
    else:
        logger.warning(f"Skills data for job '{title}' is not a list: {skills}")

    existing_job_keys.add(job_key)

    logger.debug(f"Prepared new job for addition: {new_job.title} at {new_job.company_name}")
    return new_job

# Kafka Message Handling
def send_status_update(producer, topic: str, job_id: str, event_type: str, **kwargs):
    """Helper to send status/notification messages via Kafka."""
    message = {
        "job_id": job_id,
        "event_type": event_type,
        "source": "loader",
        "timestamp": time.time()
    }
    if "percentage" in kwargs:
        message["percentage"] = round(float(kwargs["percentage"]), 2)
    if "description" in kwargs:
        message["description"] = str(kwargs["description"])

    message.update({k: v for k, v in kwargs.items() if k not in ["percentage", "description"]})

    logger.debug(f"Sending Kafka message to {topic}: {message}")
    send_message(producer, topic, message)


# Main Processing Logic
def process_loading_request(producer, job_id: str):
    """Handles a loading request for a specific job ID with descriptive progress."""
    logger.info(f"[Job {job_id}] Processing loading request...")
    current_percentage = 90.0

    file_path = os.path.join(DATA_DIR, f"{job_id}_jobs.json")

    job_listings = load_transformed_data(file_path, job_id)
    if job_listings is None:
        error_msg = f"Failed to load or find data file: {file_path}"
        logger.error(f"[Job {job_id}] {error_msg}")
        send_status_update(producer, SYSTEM_NOTIFICATIONS_TOPIC, job_id, "job_failed",
                           error_details=error_msg)
        send_status_update(producer, JOB_STATUS_UPDATES_TOPIC, job_id, "job_progress",
                           percentage=current_percentage, description=f"Failed: {error_msg}")
        return

    total_jobs_in_file = len(job_listings)
    if total_jobs_in_file == 0:
        logger.warning(f"[Job {job_id}] No job listings found in file {file_path}. Marking as complete.")
        send_status_update(producer, JOB_STATUS_UPDATES_TOPIC, job_id, "loading_complete", # Use loading_complete
                           percentage=100.0, description="Successfully loaded 0 new jobs (empty file).")
        try:
            os.remove(file_path)
            logger.info(f"[Job {job_id}] Deleted empty processed file: {file_path}")
        except OSError as e:
            logger.error(f"[Job {job_id}] Error deleting empty file {file_path}: {e}")
            send_status_update(producer, SYSTEM_NOTIFICATIONS_TOPIC, job_id, "system_warning",
                               details=f"Failed to delete empty file: {file_path}. Error: {e}")
        return

    # Database Connection
    if not DATABASE_URL:
        error_msg = "DATABASE_URL not configured in loader service"
        logger.error(f"[Job {job_id}] {error_msg}")
        send_status_update(producer, SYSTEM_NOTIFICATIONS_TOPIC, job_id, "job_failed",
                           error_details=error_msg)
        send_status_update(producer, JOB_STATUS_UPDATES_TOPIC, job_id, "job_progress",
                           percentage=current_percentage, description=f"Failed: {error_msg}")
        return

    engine = None
    session = None
    try:
        logger.info(f"[Job {job_id}] Connecting to database...")
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
             logger.info(f"[Job {job_id}] Database connection successful.")
        Session = sessionmaker(bind=engine)
        session = Session()
    except sqlalchemy_exc.SQLAlchemyError as e:
        error_msg = f"Database connection/initialization error: {e}"
        logger.error(f"[Job {job_id}] {error_msg}", exc_info=True)
        send_status_update(producer, SYSTEM_NOTIFICATIONS_TOPIC, job_id, "job_failed",
                           error_details=error_msg)
        send_status_update(producer, JOB_STATUS_UPDATES_TOPIC, job_id, "job_progress",
                           percentage=current_percentage, description=f"Failed: {error_msg}")
        if engine: engine.dispose()
        return
    except Exception as e:
        error_msg = f"Unexpected error during DB setup: {e}"
        logger.error(f"[Job {job_id}] {error_msg}", exc_info=True)
        send_status_update(producer, SYSTEM_NOTIFICATIONS_TOPIC, job_id, "job_failed",
                           error_details=error_msg)
        send_status_update(producer, JOB_STATUS_UPDATES_TOPIC, job_id, "job_progress",
                           percentage=current_percentage, description=f"Failed: {error_msg}")
        if session: session.close()
        if engine: engine.dispose()
        return

    # Processing Loop
    new_jobs_count = 0
    processed_count = 0
    duplicates_count = 0
    objects_to_add = []

    try:
        logger.info(f"[Job {job_id}] Fetching existing job identifiers from database...")
        existing_jobs_query = session.query(Job.title, Job.company_name).distinct()
        existing_job_keys = set((title.strip().lower(), company.strip().lower()) for title, company in existing_jobs_query)
        logger.info(f"[Job {job_id}] Found {len(existing_job_keys)} existing unique job identifiers.")

        # Update via kafka
        current_percentage = 91.0
        prep_message = f"Preparing to load {total_jobs_in_file} potential jobs into the database..."
        logger.info(f"[Job {job_id}] {prep_message}")
        send_status_update(producer, JOB_STATUS_UPDATES_TOPIC, job_id, "job_progress",
                           percentage=current_percentage, description=prep_message)

        for index, job_data in enumerate(job_listings):
            processed_count += 1
            new_job_object = process_job_listing(session, job_data, existing_job_keys)

            if new_job_object:
                objects_to_add.append(new_job_object)
                new_jobs_count += 1
            elif job_data.get("title") and job_data.get("company"):
                duplicates_count += 1

        # Add all new objects in bulk
        if objects_to_add:
            logger.info(f"[Job {job_id}] Adding {len(objects_to_add)} new job objects to session...")
            session.add_all(objects_to_add)
        else:
            logger.info(f"[Job {job_id}] No new job objects to add to session.")

        # Update via kafka
        current_percentage = 98.0
        if duplicates_count > 0:
            commit_message = f"Identified {duplicates_count} duplicates. Preparing to commit {new_jobs_count} new jobs..."
        else:
            commit_message = f"Preparing to commit {new_jobs_count} new jobs..."
        logger.info(f"[Job {job_id}] {commit_message}")
        send_status_update(producer, JOB_STATUS_UPDATES_TOPIC, job_id, "job_progress",
                           percentage=current_percentage, description=commit_message)

        logger.info(f"[Job {job_id}] Committing transaction...")
        session.commit()
        logger.info(f"[Job {job_id}] Successfully committed {new_jobs_count} new jobs (processed {processed_count} from file).")

        # Update via kafka
        current_percentage = 100.0
        final_message = f"Successfully loaded {new_jobs_count} new jobs into the database."
        send_status_update(producer, JOB_STATUS_UPDATES_TOPIC, job_id, "loading_complete", # Use loading_complete
                           percentage=current_percentage, description=final_message)

        # Cleanup
        try:
            logger.info(f"[Job {job_id}] Attempting to delete processed file: {file_path}")
            os.remove(file_path)
            logger.info(f"[Job {job_id}] Deleted processed file: {file_path}")
        except OSError as e:
            logger.error(f"[Job {job_id}] Error deleting processed file {file_path}: {e}")
            send_status_update(producer, SYSTEM_NOTIFICATIONS_TOPIC, job_id, "system_warning",
                               details=f"Failed to delete processed file: {file_path}. Error: {e}")

    except Exception as e:
        error_msg = f"Error during data loading/commit: {type(e).__name__} - {e}"
        logger.error(f"[Job {job_id}] {error_msg}", exc_info=True)
        if session:
            logger.warning(f"[Job {job_id}] Rolling back transaction due to error.")
            session.rollback()
        send_status_update(producer, SYSTEM_NOTIFICATIONS_TOPIC, job_id, "job_failed",
                           error_details=error_msg)
        send_status_update(producer, JOB_STATUS_UPDATES_TOPIC, job_id, "job_progress",
                           percentage=current_percentage, description=f"Failed: {error_msg}")
    finally:
        if session:
            session.close()
            logger.debug(f"[Job {job_id}] Database session closed.")
        if engine:
            engine.dispose()
            logger.debug(f"[Job {job_id}] Database engine disposed.")

# Main Consumer Loop
def main_consumer_loop():
    """Main loop to consume messages from Kafka and trigger processing."""
    logger.info("Starting Loader Service...")
    producer = None
    consumer = None
    try:
        producer = create_kafka_producer()
        consumer = create_kafka_consumer(DATA_PROCESSING_TOPIC, LOADER_CONSUMER_GROUP_ID)

        if not producer or not consumer:
            logger.critical("Failed to initialize Kafka producer or consumer. Exiting.")
            sys.exit(1)

        logger.info(f"Loader service ready. Listening for messages on topic '{DATA_PROCESSING_TOPIC}'...")
        for message in consumer:
            msg_data = message.value
            job_id = "unknown"

            try:
                if not isinstance(msg_data, dict):
                     logger.error(f"Received non-dictionary message: {type(msg_data)} - {str(msg_data)[:200]}...")
                     continue

                job_id = msg_data.get("job_id", "unknown")
                event_type = msg_data.get("event_type")
                source = msg_data.get("source")

                logger.info(f"[Job {job_id}] Received message: Event='{event_type}', Source='{source}', Offset={message.offset}")

                if event_type == "loading_requested" and job_id != "unknown" and source == "scraper":
                    logger.info(f"[Job {job_id}] Received valid '{event_type}' from '{source}'. Triggering processing...")
                    process_loading_request(producer, job_id)
                elif event_type == "loading_requested":
                     logger.warning(f"[Job {job_id}] Received '{event_type}' but source ('{source}') is not 'scraper' or job_id is missing. Skipping.")
                else:
                    logger.warning(f"[Job {job_id}] Received unexpected event_type '{event_type}'. Skipping.")

            except Exception as e:
                logger.error(f"[Job {job_id}] Unhandled error processing Kafka message (Offset: {message.offset}): {e}", exc_info=True)
                if producer and job_id != "unknown":
                    try:
                        send_status_update(producer, SYSTEM_NOTIFICATIONS_TOPIC, job_id, "job_failed",
                                           error_details=f"Internal loader error processing message: {e}")
                        send_status_update(producer, JOB_STATUS_UPDATES_TOPIC, job_id, "job_progress",
                                           percentage=90.0,
                                           description=f"Failed: Internal loader error")
                    except Exception as send_e:
                         logger.error(f"[Job {job_id}] Failed to send error notification for internal error: {send_e}")

    except KeyboardInterrupt:
        logger.info("Consumer loop interrupted by user (KeyboardInterrupt). Shutting down...")
    except Exception as e:
        logger.critical(f"Critical error in main consumer loop: {e}", exc_info=True)
    finally:
        logger.info("Closing Kafka consumer and producer...")
        if consumer:
            try:
                consumer.close()
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
        if producer:
            try:
                producer.flush(timeout=5)
                producer.close()
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
        logger.info("Loader service stopped.")

if __name__ == "__main__":
    if not DATABASE_URL:
        logger.critical("FATAL: DATABASE_URL environment variable not set at startup.")
        sys.exit(1)
    main_consumer_loop()