import json
import os
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
from logger.logger import get_logger
from typing import Optional, Any, Dict

logger = get_logger("scraper_kafka_client")

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "kafka:9092")
MAX_CONNECTION_RETRIES = 5
RETRY_DELAY_SECONDS = 5


def get_kafka_producer() -> Optional[KafkaProducer]:
    """
    Creates and returns a KafkaProducer instance.
    Retries connection if brokers are not available initially.
    """
    retries = 0
    while retries < MAX_CONNECTION_RETRIES:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER_URL,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info(f"Kafka Producer connected successfully to {KAFKA_BROKER_URL}")
            return producer
        except NoBrokersAvailable:
            retries += 1
            logger.warning(
                f"Kafka brokers not available at {KAFKA_BROKER_URL}. Retrying ({retries}/{MAX_CONNECTION_RETRIES}) in {RETRY_DELAY_SECONDS}s..."
            )
            if retries >= MAX_CONNECTION_RETRIES:
                logger.error("Max retries reached. Failed to connect Kafka Producer.")
                return None
            time.sleep(RETRY_DELAY_SECONDS)
        except Exception as e:
            logger.error(f"An unexpected error occurred creating Kafka producer: {e}")
            return None


def get_kafka_consumer(topic: str, group_id: str) -> Optional[KafkaConsumer]:
    """
    Creates and returns a KafkaConsumer instance subscribed to a topic.
    Retries connection if brokers are not available initially.
    """
    retries = 0
    while retries < MAX_CONNECTION_RETRIES:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKER_URL,
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            logger.info(
                f"Kafka Consumer connected successfully to {KAFKA_BROKER_URL}, subscribed to topic '{topic}' with group_id '{group_id}'"
            )
            return consumer
        except NoBrokersAvailable:
            retries += 1
            logger.warning(
                f"Kafka brokers not available at {KAFKA_BROKER_URL}. Retrying consumer ({retries}/{MAX_CONNECTION_RETRIES}) in {RETRY_DELAY_SECONDS}s..."
            )
            if retries >= MAX_CONNECTION_RETRIES:
                logger.error("Max retries reached. Failed to connect Kafka Consumer.")
                return None
            time.sleep(RETRY_DELAY_SECONDS)
        except Exception as e:
            logger.error(f"An unexpected error occurred creating Kafka consumer: {e}")
            return None


def send_kafka_message(
    producer: KafkaProducer,
    topic: str,
    message: Dict[str, Any],
    job_id: Optional[str] = None,
) -> bool:
    """
    Sends a message to a specified Kafka topic using the provided producer.

    Args:
        producer: The KafkaProducer instance.
        topic: The target Kafka topic.
        message: The message payload (dictionary).
        job_id: Optional job ID to include in logging.

    Returns:
        True if the message was sent successfully, False otherwise.
    """
    if not producer:
        logger.error(
            f"Cannot send message to topic '{topic}', producer is not available."
        )
        return False

    if "timestamp" not in message:
        message["timestamp"] = time.time()

    log_prefix = f"[Job {job_id}] " if job_id else ""

    try:
        future = producer.send(topic, value=message)
        record_metadata = future.get(timeout=10)
        logger.info(
            f"{log_prefix}Message sent successfully to topic '{topic}' at offset {record_metadata.offset}"
        )
        producer.flush()
        return True
    except Exception as e:
        logger.error(f"{log_prefix}Failed to send message to topic '{topic}': {e}")
        return False
