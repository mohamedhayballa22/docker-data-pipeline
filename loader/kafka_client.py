import json
import os
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
from logger.logger import get_logger

logger = get_logger("loader_kafka_client")

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "kafka:9092")
LOADER_CONSUMER_GROUP_ID = "loader-group" 
DATA_PROCESSING_TOPIC = "data-processing"
JOB_STATUS_UPDATES_TOPIC = "job-status-updates"
SYSTEM_NOTIFICATIONS_TOPIC = "system-notifications"


def create_kafka_producer():
    """Creates and returns a KafkaProducer instance."""
    retries = 5
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER_URL,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info("Kafka Producer connected successfully.")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka brokers not available. Retrying in 5 seconds... ({retries} retries left)")
            retries -= 1
            time.sleep(5)
        except Exception as e:
            logger.error(f"Failed to create Kafka Producer: {e}")
            raise
    logger.error("Failed to connect to Kafka Producer after multiple retries.")
    return None


def create_kafka_consumer(topic, group_id):
    """Creates and returns a KafkaConsumer instance for the specified topic and group."""
    retries = 5
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKER_URL,
                group_id=group_id,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True, 
                auto_commit_interval_ms=5000 
            )
            logger.info(f"Kafka Consumer connected successfully for topic '{topic}'.")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"Kafka brokers not available for consumer. Retrying in 5 seconds... ({retries} retries left)")
            retries -= 1
            time.sleep(5)
        except Exception as e:
            logger.error(f"Failed to create Kafka Consumer for topic '{topic}': {e}")
            raise
    logger.error(f"Failed to connect Kafka Consumer for topic '{topic}' after multiple retries.")
    return None


def send_message(producer, topic, message_data):
    """Sends a message to the specified Kafka topic."""
    if not producer:
        logger.error(f"Producer not available. Cannot send message to {topic}: {message_data}")
        return False
    try:
        if "timestamp" not in message_data:
             message_data["timestamp"] = time.time()

        future = producer.send(topic, value=message_data)
        producer.flush()
        logger.info(f"Sent message to {topic}: {message_data.get('event_type', 'N/A')} for job {message_data.get('job_id', 'N/A')}")
        return True
    except Exception as e:
        logger.error(f"Failed to send message to {topic}: {e}")
        logger.error(f"Message data: {message_data}")
        return False