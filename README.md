# Data-Engineer-Challenge
 This repository provides a proof-of-concept for a real-time data ingestion app to process and analyze cryptocurrency market data from Coinbase. It supports Kahoot!'s finance team with real-time insights, including highest bid, lowest ask, max spread, and mid-price forecasting. Built in Python with PostgreSQL for efficient updates.

 ## Project Structure
Data-Engineer-Challenge/
├── src/
│   ├── data_stream.py       # Simulated data producer for Kafka
│   ├── calculations.py      # Data consumer with metrics calculations
├── docs/
│   ├── assumptions.md
│   ├── architecture_diagram.png
│   ├── design_decisions.md
├── tests/
│   ├── test_data_stream.py
│   ├── test_calculations.py
├── requirements.txt
├── README.md
└── .gitignore

# Setup
Prerequisites
- Kafka: Ensure Kafka and Zookeeper are installed and running.
- Python 3.8+: Install Python and set up a virtual environment.
- Required Python Packages: Install dependencies using requirements.txt.
    pip install -r requirements.txt

Running the Data Stream (Producer)
1) Ensure Kafka is running.
2) Start the data stream:
    python src/data_stream.py
This will produce simulated cryptocurrency bid and ask prices and send them to the crypto_prices Kafka topic every 5 seconds.

Running the Consumer with Calculations
1) Ensure the data stream is running and Kafka is producing data to the crypto_prices topic.
2) Start the consumer:
    python src/calculations.py

The consumer will read data from Kafka and calculate:
- Highest Bid
- Lowest Ask
- Max Spread
- Mid Price
These metrics are displayed in real-time in the terminal.

# Configuration
- Kafka Topic: crypto_prices (default).
- Kafka Server: localhost:9092 (default).
Update these settings in data_stream.py and calculations.py as needed.

# Error Handling
The consumer handles common errors such as partition EOF and retries on connection issues, ensuring resilience in the event of network disruptions.