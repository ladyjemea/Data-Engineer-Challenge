from confluent_kafka import Producer
import json
import time
import random
import logging


KAFKA_TOPIC = 'crypto_prices'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
CURRENCY_PAIRS = ['BTC-USD', 'ETH-USD', 'LTC-USD']

def generate_price_data():
    """Simulate fetching or generating data similar to Coinbase API data"""
    data = []
    for pair in CURRENCY_PAIRS:
        bid_price = round(random.uniform(30000, 60000), 2)
        ask_price = round(bid_price + random.uniform(50, 150), 2)
        price_level = round(random.uniform(1, 10), 2) 
        quantity = round(random.uniform(0.1, 5), 2)   

        entry = {
            "pair": pair,                   
            "bid": bid_price,               
            "ask": ask_price,              
            "price_level": price_level,     
            "quantity": quantity,           
            "timestamp": time.time()        
        }
        
        data.append(entry)
    logging.info(f"Generated data: {data}")
    return data

def send_data():
    """Send generated data to Kafka."""
    while True:
        data = generate_price_data()
        for record in data:
            producer.produce(KAFKA_TOPIC, json.dumps(record).encode('utf-8'))
            logging.info(f"Sent to Kafka: {record}")  
        producer.flush()
        time.sleep(5)  

if __name__ == '__main__':
    print("Starting data stream...")
    send_data()