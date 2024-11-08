from confluent_kafka import Producer
import json
import time
import random

# Kafka configuration
KAFKA_TOPIC = 'crypto_prices'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Initialize Kafka Producer
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

# Currency pairs we want to track
CURRENCY_PAIRS = ['BTC-USD', 'ETH-USD', 'LTC-USD']

def generate_price_data():
    """Simulate fetching or generating data similar to Coinbase API order book feeds."""
    data = []
    for pair in CURRENCY_PAIRS:
        # Generate random prices for bid and ask
        bid_price = round(random.uniform(30000, 60000), 2)
        ask_price = round(bid_price + random.uniform(50, 150), 2)
        
        # Simulate additional fields
        price_level = round(random.uniform(1, 10), 2)  # Price level as an example field
        quantity = round(random.uniform(0.1, 5), 2)    # Quantity of the order

        # Create a data entry
        entry = {
            "pair": pair,                   # Currency pair (e.g., BTC-USD)
            "bid": bid_price,               # Bid price for the currency pair
            "ask": ask_price,               # Ask price for the currency pair
            "price_level": price_level,     # Simulated price level field
            "quantity": quantity,           # Simulated quantity field
            "timestamp": time.time()        # Current timestamp
        }
        
        data.append(entry)
    return data

def send_data():
    """Send generated data to Kafka."""
    while True:
        data = generate_price_data()
        for record in data:
            producer.produce(KAFKA_TOPIC, json.dumps(record).encode('utf-8'))
            print(f"Sent to Kafka: {record}")  # Log each message sent
        producer.flush()
        time.sleep(5)  # Send data every 5 seconds to simulate a real-time feed

if __name__ == '__main__':
    print("Starting data stream...")
    send_data()