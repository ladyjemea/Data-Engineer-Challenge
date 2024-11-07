import logging
import time
import psycopg2
from confluent_kafka import KafkaException

logging.basicConfig(filename='pipeline.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

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

def log_error(error_message):
    """Log an error message."""
    logging.error(error_message)
