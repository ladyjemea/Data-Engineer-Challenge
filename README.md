# Data-Engineer-Challenge
This repository contains a solution for a real-time data processing pipeline designed to ingest cryptocurrency market data from Coinbase, perform calculations, and store insights for further analysis. The project simulates real-time market data ingestion using Apache Kafka and processes the data to track key metrics such as bid, ask, spread, and mid-price.

## Table of Contents
1) Project Overview
2) Architecture
3) Setup and Installation
4) Running the Project
5) Project Structure
6) Testing
7) Assumptions
8) Design Decisions
9) Future Improvements

## Project Overview
The objective of this project is to provide a real-time data ingestion and processing solution for cryptocurrency market data. The project:
- Simulates real-time market data from Coinbase for several currency pairs.
- Ingests data via Kafka, calculates metrics (such as bid, ask, spread, and mid-price), and stores the results in a PostgreSQL database.
- Tracks the highest bid, lowest ask, and maximum spread across the session.
This setup helps a finance team make timely cryptocurrency risk decisions by providing continuous insights into market trends.

## Architecture
The project consists of the following components:
1) Data Ingestion Layer: Simulates real-time market data generation and sends it to Kafka.
2) Kafka Stream: Serves as a message broker, facilitating the data stream from the ingestion layer to the processing layer.
3) Processing Layer: Consumes data from Kafka, calculates metrics, and stores the results in a PostgreSQL database.
4) Database Storage: Stores calculated metrics in PostgreSQL for easy retrieval and analysis.

The high-level data flow:
1) data_stream.py generates simulated data for cryptocurrency pairs and publishes it to a Kafka topic.
2) calculations.py consumes the Kafka data, calculates metrics, and saves results to PostgreSQL.
3) data_processing.py provides a foundation for future expansions, such as windowing-based aggregations using Apache Beam.

## Setup and Installation
Prerequisites
- Python 3.8+ (Python 3.8 is recommended due to compatibility issues with some libraries).
- Apache Kafka installed and running locally or accessible from the application.
- PostgreSQL database set up with a crypto_metrics table.
- Virtual environment for isolating dependencies.

Installation
1) Clone the Repository
    - git clone https://github.com/ladyjemea/Data-Engineer-Challenge.git
    - cd Data-Engineer-Challenge
2) Set Up Python Virtual Environment
    - python3 -m venv venv
    - source venv/bin/activate
3) Install Required Packages
    - pip install -r requirements.txt
4) Configure Environment Variables
    - Create a .env file at the root of the project and set the following variables:
    DB_NAME=your_db_name
    DB_USER=your_db_user
    DB_PASSWORD=your_db_password
    DB_HOST=localhost
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092
5) Set Up PostgreSQL Table
    - Run the following SQL command to set up the crypto_metrics table:
    CREATE TABLE crypto_metrics (
        id SERIAL PRIMARY KEY,
        pair VARCHAR(10),
        bid NUMERIC,
        ask NUMERIC,
        mid_price NUMERIC,
        spread NUMERIC,
        highest_bid NUMERIC,
        lowest_ask NUMERIC,
        max_spread NUMERIC,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

## Running the Project
1) Start Kafka:
    - Ensure that Kafka is running locally and has a topic named crypto_prices.
    - Create the topic if it doesn’t exist:
    bin/kafka-topics.sh --create --topic crypto_prices --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

2) Run Data Stream (Data Ingestion)
    - This script simulates real-time data and publishes it to Kafka.
    python src/data_stream.py

3) Run Calculations (Data Processing)
    - This script consumes data from Kafka, calculates metrics, and saves them to PostgreSQL.
    python src/calculations.py

## Project Structure
Data-Engineer-Challenge/
├── src/
│   ├── data_stream.py       # Simulates data ingestion and publishes to Kafka
│   ├── calculations.py      # Consumes Kafka messages, processes data, and saves to DB
│   ├── data_processing.py   # Future extension for windowing and aggregation
├── docs/
│   ├── assumptions.md       # Documented assumptions for the project
│   ├── architecture_diagram.png # Architecture diagram
│   ├── design_decisions.md  # Documented design decisions
├── tests/
│   ├── test_data_stream.py  # Tests for data_stream
│   ├── test_calculations.py # Tests for calculations
├── requirements.txt         # Python package dependencies
├── README.md                # Project documentation
└── .env.example             # Example environment variables file

## Testing
Unit tests are included for the main components (data_stream.py and calculations.py). To run tests, execute the following command:
   - PYTHONPATH=. python -m unittest discover tests
The tests include:
- Data Stream Tests: Verify that data is generated correctly in the expected format.
- Calculations Tests: Test that metrics are calculated accurately and database insertion is performed as expected.

## Assumptions
This project makes several assumptions, such as a consistent data schema, available Kafka topic, and specific PostgreSQL table structure. For a detailed list of assumptions, see assumptions.md.

## Design Decisions
The design decisions for this project were made to balance real-time data processing, system reliability, and modularity. For a complete overview, refer to design_decisions.md.

## Future Improvements
- Enhanced Error Handling: Add more robust error handling and alerting mechanisms.
- Scalability: Use consumer groups to distribute data processing across multiple instances.
- Windowed Aggregations: Implement windowing in data_processing.py to support time-based aggregations.
- Data Visualization: Integrate with BI tools (e.g., Grafana or Tableau) for real-time data visualization.