from confluent_kafka import Consumer, KafkaError
import json

# Kafka Configuration
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

def calculate_metrics(data):
    """Calculate highest bid, lowest ask, max spread, and mid-price."""
    global highest_bid, lowest_ask, max_spread

    bid = data['bid']
    ask = data['ask']
    mid_price = (bid + ask) / 2
    spread = ask - bid

    # Update metrics
    highest_bid = max(highest_bid, bid)
    lowest_ask = min(lowest_ask, ask)
    max_spread = max(max_spread, spread)

    # Display metrics
    print(f"Currency Pair: {data['pair']}")
    print(f"Current Bid: {bid}, Ask: {ask}, Mid Price: {mid_price}")
    print(f"Highest Bid: {highest_bid}, Lowest Ask: {lowest_ask}, Max Spread: {max_spread}")
    print("-" * 40)

def consume_data():
    """Consume data from Kafka and calculate metrics."""
    print("Starting the consumer and calculations...")
    while True:
        message = consumer.poll(1.0)  # Poll every 1 second

        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print(f"End of partition reached {message.topic()} [{message.partition()}] at offset {message.offset()}")
            elif message.error():
                print(f"Error: {message.error()}")
            continue

        # Process and calculate metrics
        data = json.loads(message.value().decode('utf-8'))
        calculate_metrics(data)

if __name__ == '__main__':
    try:
        consume_data()
    except KeyboardInterrupt:
        print("Consumer interrupted.")
    finally:
        consumer.close()
