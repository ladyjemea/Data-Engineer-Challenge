from confluent_kafka import Consumer, KafkaError
import json
import psycopg2
from psycopg2 import sql
import logging
import os
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST')
}

# Kafka configuration
KAFKA_TOPIC = 'crypto_prices'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Initialize Kafka Consumer
consumer = Consumer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'crypto_price_consumer_group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([KAFKA_TOPIC])

# Metrics Tracking Variables
highest_bid = 0
lowest_ask = float('inf')
max_spread = 0

def connect_to_db():
    """Establish a connection to the PostgreSQL database with retry logic."""
    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as e:
            logging.error(f"Database connection error: {e}. Retrying...")
            retries -= 1
            time.sleep(2)
    raise Exception("Failed to connect to the database after retries.")

def save_metrics_to_db(data, conn):
    """Save calculated metrics to the PostgreSQL database."""
    try:
        with conn.cursor() as cur:
            query = """
                INSERT INTO crypto_metrics (pair, bid, ask, mid_price, spread, highest_bid, lowest_ask, max_spread)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(query, (
                data['pair'],
                data['bid'],
                data['ask'],
                data['mid_price'],
                data['spread'],
                data['highest_bid'],
                data['lowest_ask'],
                data['max_spread']
            ))
            conn.commit()  # Commit to save the data in the database
            print(f"Metrics saved to database for pair {data['pair']}.")
    except Exception as e:
        logging.error(f"Error saving metrics to database: {e}")

def calculate_metrics(data, conn):
    """Calculate highest bid, lowest ask, max spread, and mid-price, and save to DB."""
    global highest_bid, lowest_ask, max_spread

    bid = data['bid']
    ask = data['ask']
    mid_price = (bid + ask) / 2
    spread = ask - bid

    # Update metrics
    highest_bid = max(highest_bid, bid)
    lowest_ask = min(lowest_ask, ask)
    max_spread = max(max_spread, spread)

    # Prepare data to be saved
    data_to_save = {
        'pair': data['pair'],
        'bid': bid,
        'ask': ask,
        'mid_price': mid_price,
        'spread': spread,
        'highest_bid': highest_bid,
        'lowest_ask': lowest_ask,
        'max_spread': max_spread
    }

    # Save metrics to database
    save_metrics_to_db(data_to_save, conn)

def consume_data():
    """Consume data from Kafka, calculate metrics, and store in DB."""
    print("Starting the consumer and calculations...")
    conn = connect_to_db()  # Connect to the database

    while True:
        try:
            message = consumer.poll(1.0)  # Poll every 1 second

            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {message.topic()} [{message.partition()}] at offset {message.offset()}")
                else:
                    logging.error(f"Error: {message.error()}")
                continue

            # Process and calculate metrics
            data = json.loads(message.value().decode('utf-8'))
            calculate_metrics(data, conn)
        
        except Exception as e:
            logging.error(f"Error during data consumption: {e}")

    conn.close()  # Close the connection to the database

if __name__ == '__main__':
    try:
        consume_data()
    except KeyboardInterrupt:
        print("Consumer interrupted.")
    finally:
        consumer.close()
