import sys
from pathlib import Path
from confluent_kafka import Consumer, KafkaError
import json
import logging
import os
import psycopg2
from dotenv import load_dotenv

# Add the project root directory to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.error_handling import connect_to_database, retry, log_error

# Load environment variables
load_dotenv()

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST')
}

KAFKA_TOPIC = 'crypto_prices'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

consumer = Consumer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'crypto_price_consumer_group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([KAFKA_TOPIC])

# Global metrics
highest_bid = 0
lowest_ask = float('inf')
max_spread = 0
window_size = 5  
mid_prices = []

@retry
def save_metrics_to_db(data: dict, conn):
    """Save calculated metrics to the PostgreSQL database."""
    try:
        with conn.cursor() as cur:
            query = """
                INSERT INTO crypto_metrics (pair, bid, ask, mid_price, spread, highest_bid, lowest_ask, max_spread, moving_avg)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(query, (
                data['pair'],
                data['bid'],
                data['ask'],
                data['mid_price'],
                data['spread'],
                data['highest_bid'],
                data['lowest_ask'],
                data['max_spread'],
                data['moving_avg']
            ))
            conn.commit()
            logging.info(f"Metrics saved to database for pair {data['pair']}.")
    except Exception as e:
        log_error(f"Error saving metrics to database: {e}")

def validate_and_extract_data(data: dict) -> dict:
    """Validate and extract required fields from incoming data."""
    try:
        return {
            'pair': data['pair'],
            'bid': float(data['bid']),
            'ask': float(data['ask'])
        }
    except (ValueError, KeyError) as e:
        log_error(f"Malformed data: {data}. Error: {e}")
        return None

def calculate_mid_price(bid: float, ask: float) -> float:
    """Calculate the mid-price."""
    return (bid + ask) / 2

def calculate_spread(bid: float, ask: float) -> float:
    """Calculate the spread."""
    return ask - bid

def update_cumulative_metrics(bid: float, ask: float):
    """Update highest bid, lowest ask, and max spread."""
    global highest_bid, lowest_ask, max_spread
    highest_bid = max(highest_bid, bid)
    lowest_ask = min(lowest_ask, ask)
    max_spread = max(max_spread, calculate_spread(bid, ask))

def calculate_moving_average(mid_price: float) -> float:
    """Update and calculate the moving average of mid prices."""
    global mid_prices
    mid_prices.append(mid_price)
    if len(mid_prices) > window_size:
        mid_prices.pop(0)
    return sum(mid_prices) / len(mid_prices) if mid_prices else 0

def prepare_data_for_db(pair: str, bid: float, ask: float) -> dict:
    """Prepare a dictionary of metrics for database insertion."""
    mid_price = calculate_mid_price(bid, ask)
    spread = calculate_spread(bid, ask)
    update_cumulative_metrics(bid, ask)
    moving_avg = calculate_moving_average(mid_price)
    return {
        'pair': pair,
        'bid': bid,
        'ask': ask,
        'mid_price': mid_price,
        'spread': spread,
        'highest_bid': highest_bid,
        'lowest_ask': lowest_ask,
        'max_spread': max_spread,
        'moving_avg': moving_avg
    }

def process_message(data: dict, conn):
    """Process a single message by validating, calculating metrics, and saving to the database."""
    validated_data = validate_and_extract_data(data)
    if not validated_data:
        return

    db_data = prepare_data_for_db(
        validated_data['pair'],
        validated_data['bid'],
        validated_data['ask']
    )
    save_metrics_to_db(db_data, conn)

def consume_data():
    """Consume data from Kafka, process it, and save metrics to the database."""
    logging.info("Starting the consumer and calculations...")

    try:
        conn = connect_to_database(DB_CONFIG)
    except Exception as e:
        log_error(f"Database connection failed: {e}")
        exit(1)

    while True:
        try:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    logging.info(f"End of partition reached at offset {message.offset()}")
                else:
                    log_error(f"Kafka error: {message.error()}")
                continue

            data = json.loads(message.value().decode('utf-8'))
            logging.info(f"Received message: {data}")
            process_message(data, conn)

        except Exception as e:
            log_error(f"Error during data consumption: {e}")

    conn.close()

if __name__ == '__main__':
    try:
        consume_data()
    except KeyboardInterrupt:
        logging.info("Consumer interrupted.")
    finally:
        consumer.close()