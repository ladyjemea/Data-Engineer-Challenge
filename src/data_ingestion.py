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

# Initialize Kafka Producer
try:
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    logging.info("Kafka Producer initialized successfully.")
except Exception as e:
    logging.error(f"Failed to initialize Kafka Producer: {e}")
    raise e

# Simulated data
CURRENCY_PAIRS = ['BTC-USD', 'ETH-USD', 'LTC-USD']

def fetch_data():
    """Simulate fetching or generating data."""
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
    logging.info(f"Fetched data: {data}")
    return data

def send_data_to_kafka(data):
    """Send data to Kafka topic."""
    try:
        for record in data:
            producer.produce(KAFKA_TOPIC, json.dumps(record).encode('utf-8'))
            logging.info(f"Sent to Kafka: {record}")
        producer.flush()
    except Exception as e:
        logging.error(f"Error sending data to Kafka: {e}")

def main():
    """Main function to fetch and send data periodically."""
    while True:
        data = fetch_data()
        send_data_to_kafka(data)
        time.sleep(5)

if __name__ == '__main__':
    try:
        logging.info("Starting data ingestion...")
        main()
    except KeyboardInterrupt:
        logging.info("Data ingestion stopped by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")