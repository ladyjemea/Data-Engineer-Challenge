from confluent_kafka import Producer
import json
import time
import random
import logging

# Kafka Configuration
KAFKA_TOPIC = 'crypto_prices'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Initialize Kafka Producer
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

# Currency pairs for simulation
CURRENCY_PAIRS = ['BTC-USD', 'ETH-USD', 'LTC-USD']

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)

def generate_bid_ask_prices(pair: str) -> dict:
    """
    Generate random bid and ask prices for a single currency pair.

    Parameters:
        pair (str): The currency pair (e.g., BTC-USD).

    Returns:
        dict: A dictionary containing the currency pair, bid price, ask price, and timestamp.
    """
    bid_price = round(random.uniform(30000, 60000), 2)
    ask_price = round(bid_price + random.uniform(50, 150), 2)
    return {
        "pair": pair,
        "bid": bid_price,
        "ask": ask_price,
        "timestamp": time.time()
    }

def generate_price_data() -> list:
    """
    Generate random bid and ask prices for all currency pairs.

    Returns:
        list: A list of dictionaries containing price data for all currency pairs.
    """
    return [generate_bid_ask_prices(pair) for pair in CURRENCY_PAIRS]

def produce_to_kafka(data: list):
    """
    Send generated data to the Kafka topic.

    Parameters:
        data (list): A list of price data dictionaries to be sent to Kafka.
    """
    for record in data:
        producer.produce(KAFKA_TOPIC, json.dumps(record).encode('utf-8'))
        logging.info(f"Sent to Kafka: {record}")
    producer.flush()

def send_data(iterations: int = None, interval: int = 5):
    """
    Generate and send price data to Kafka at regular intervals.

    Parameters:
        iterations (int, optional): Number of iterations to send data (for testing).
        interval (int, optional): Interval in seconds between data sends. Defaults to 5.
    """
    count = 0
    while iterations is None or count < iterations:
        data = generate_price_data()
        produce_to_kafka(data)
        time.sleep(interval)
        count += 1

if __name__ == '__main__':
    logging.info("Starting data stream...")
    send_data()

#add a comment