from confluent_kafka import Producer
import json
import time
import random
import logging

# Kafka Configuration
KAFKA_TOPIC = 'crypto_prices'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Configure logging to print to the console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Currency pairs for data simulation
CURRENCY_PAIRS = ['BTC-USD', 'ETH-USD', 'LTC-USD']

def initialize_kafka_producer():
    """
    Initialize the Kafka producer.
    
    Returns:
        Producer: A Kafka producer instance.
    """
    try:
        producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        logging.info("Kafka Producer initialized successfully.")
        return producer
    except Exception as e:
        logging.error(f"Failed to initialize Kafka Producer: {e}")
        raise e

def generate_price_data(pair):
    """
    Generate bid and ask prices for a single currency pair.

    Parameters:
        pair (str): The currency pair (e.g., BTC-USD).

    Returns:
        dict: Simulated bid/ask data for the currency pair.
    """
    bid_price = round(random.uniform(30000, 60000), 2)
    ask_price = round(bid_price + random.uniform(50, 150), 2)
    return {
        "pair": pair,
        "bid": bid_price,
        "ask": ask_price,
        "timestamp": time.time()
    }

def fetch_data():
    """
    Simulate fetching data for all currency pairs.

    Returns:
        list: A list of dictionaries containing bid/ask data for all pairs.
    """
    data = [generate_price_data(pair) for pair in CURRENCY_PAIRS]
    logging.info(f"Fetched data: {data}")
    return data

def send_data_to_kafka(producer, data):
    """
    Send data to a Kafka topic.

    Parameters:
        producer (Producer): The Kafka producer instance.
        data (list): A list of bid/ask data dictionaries.
    """
    try:
        for record in data:
            producer.produce(KAFKA_TOPIC, json.dumps(record).encode('utf-8'))
            logging.info(f"Sent to Kafka: {record}")
        producer.flush()
    except Exception as e:
        logging.error(f"Error sending data to Kafka: {e}")

def run_data_ingestion(producer, interval=5):
    """
    Continuously fetch and send data to Kafka.

    Parameters:
        producer (Producer): The Kafka producer instance.
        interval (int): The interval in seconds between data fetch/send cycles.
    """
    try:
        while True:
            data = fetch_data()
            send_data_to_kafka(producer, data)
            time.sleep(interval)
    except KeyboardInterrupt:
        logging.info("Data ingestion stopped by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == '__main__':
    producer = initialize_kafka_producer()
    logging.info("Starting data ingestion...")
    run_data_ingestion(producer)