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


def generate_price_data():
    """
    Generate random bid and ask prices for currency pairs.
    """
    data = []
    for pair in CURRENCY_PAIRS:
        bid_price = round(random.uniform(30000, 60000), 2)
        ask_price = round(bid_price + random.uniform(50, 150), 2)
        data.append({
            "pair": pair,
            "bid": bid_price,
            "ask": ask_price,
            "timestamp": time.time()
        })
    return data


def send_data(iterations=None):
    """
    Send generated price data to Kafka topic.
    Parameters:
        iterations (int): Number of iterations to send data (for testing).
    """
    count = 0
    while iterations is None or count < iterations:
        data = generate_price_data()
        for record in data:
            producer.produce(KAFKA_TOPIC, json.dumps(record).encode('utf-8'))
            logging.info(f"Sent to Kafka: {record}")
        producer.flush()
        time.sleep(5)
        count += 1


if __name__ == '__main__':
    logging.info("Starting data stream...")
    send_data()
