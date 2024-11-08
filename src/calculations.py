from confluent_kafka import Consumer, KafkaError
import json
import logging
import os
import psycopg2
from dotenv import load_dotenv
from src.error_handling import connect_to_database, retry, log_error
import logging_config

# Load environment variables
load_dotenv()

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

highest_bid = 0
lowest_ask = float('inf')
max_spread = 0
window_size = 5  
mid_prices = [] 

@retry
def save_metrics_to_db(data, conn):
    """Save calculated metrics to the PostgreSQL database with retries."""
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

def calculate_metrics(data, conn):
    """Calculate metrics and save them to the database, handling malformed data."""
    global highest_bid, lowest_ask, max_spread, mid_prices

    #Handle malformed data
    try:
        bid = float(data['bid'])
        ask = float(data['ask'])
        pair = data['pair']
    except (ValueError, KeyError) as e:
        log_error(f"Malformed data: {data}. Error: {e}")
        return

    mid_price = (bid + ask) / 2
    spread = ask - bid

    # Update cumulative metrics
    highest_bid = max(highest_bid, bid)
    lowest_ask = min(lowest_ask, ask)
    max_spread = max(max_spread, spread)

    # Update moving average
    mid_prices.append(mid_price)
    if len(mid_prices) > window_size:
        mid_prices.pop(0)

    moving_avg = sum(mid_prices) / len(mid_prices) if mid_prices else 0

    data_to_save = {
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

    save_metrics_to_db(data_to_save, conn)

def consume_data():
    """Consume data from Kafka, calculate metrics, and store in DB with error handling."""
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
            calculate_metrics(data, conn)

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