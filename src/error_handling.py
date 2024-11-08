import logging
import time
import psycopg2
from confluent_kafka import KafkaException

# Configure logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def retry(func, retries=5, delay=2):
    """Retry decorator to handle transient errors."""
    def wrapper(*args, **kwargs):
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except (psycopg2.OperationalError, KafkaException) as e:
                logging.error(f"Error in {func.__name__}: {e}")
                if attempt < retries - 1:
                    logging.info(f"Retrying {func.__name__} in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logging.error(f"{func.__name__} failed after {retries} attempts.")
                    raise e
    return wrapper

@retry
def connect_to_database(db_config):
    """Attempt to connect to the PostgreSQL database with retries."""
    return psycopg2.connect(**db_config)

@retry
def send_to_kafka(producer, topic, message):
    """Attempt to send a message to Kafka with retries."""
    producer.produce(topic, message)
    producer.flush()
    logging.info(f"Message sent to Kafka topic '{topic}': {message}")

def log_error(error_message):
    """Log an error message."""
    logging.error(error_message)