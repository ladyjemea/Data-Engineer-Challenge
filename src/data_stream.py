from confluent_kafka import Producer
import json
import time
import random

KAFKA_TOPIC = 'crypto_prices'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

CURRENCY_PAIRS = ['BTC-USD', 'ETH-USD', 'LTC-USD']

def generate_price_data():
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

def send_data():
    while True:
        data = generate_price_data()
        for record in data:
            producer.produce(KAFKA_TOPIC, json.dumps(record).encode('utf-8'))
            print(f"Sent to Kafka: {record}")
        producer.flush()
        time.sleep(5)

if __name__ == '__main__':
    print("Starting data stream...")
    send_data()