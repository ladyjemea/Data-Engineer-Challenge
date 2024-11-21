import logging
import time
import psycopg2
from confluent_kafka import KafkaException

# Configure logging to include timestamps and severity levels
logging.basicConfig(
    level=logging.ERROR, 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    force=True
)

def retry(func, retries=5, delay=2):
    """
    Retry decorator to handle transient errors.
    
    Parameters:
        func (callable): The function to be executed with retry logic.
        retries (int): Number of retry attempts.
        delay (int): Delay in seconds between retries.
    
    Returns:
        callable: The wrapped function with retry logic applied.
    """
    def wrapper(*args, **kwargs):
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except (psycopg2.OperationalError, KafkaException) as e:
                # Log the error
                logging.error(f"Error in {func.__name__}: {e}")
                if attempt < retries - 1:
                    # Retry if not the last attempt
                    logging.info(f"Retrying {func.__name__} in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                    time.sleep(delay)
                else:
                    # Exhausted all retries
                    logging.error(f"{func.__name__} failed after {retries} attempts.")
                    raise e  # Re-raise the exception for higher-level handling
    return wrapper

@retry  # Apply retry logic for database connection
def connect_to_database(db_config):
    """
    Connect to the PostgreSQL database with retries.
    
    Parameters:
        db_config (dict): Database configuration (dbname, user, password, host).
    
    Returns:
        psycopg2.connection: Active database connection.
    """
    logging.info("Attempting to connect to the database...")
    return psycopg2.connect(**db_config)

@retry  # Apply retry logic for sending messages to Kafka
def send_to_kafka(producer, topic, message):
    """
    Send a message to Kafka with retry on failure.
    
    Parameters:
        producer (confluent_kafka.Producer): Kafka producer instance.
        topic (str): Kafka topic to send the message to.
        message (str): The message to send.
    """
    logging.info(f"Sending message to Kafka topic '{topic}': {message}")
    producer.produce(topic, message)
    producer.flush()
    logging.info(f"Message successfully sent to Kafka topic '{topic}'.")

def log_error(error_message):
    """
    Log error messages for better debugging.
    
    Parameters:
        error_message (str): Error message to log.
    """
    logging.error(error_message)