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
5) Visualization Layer: Generates plots for key metrics to assist with data-driven decisions.

### The high-level data flow:
1) data_stream.py generates simulated data for cryptocurrency pairs and publishes it to a Kafka topic.
2) calculations.py consumes the Kafka data, calculates metrics, and saves results to PostgreSQL.
3) data_processing.py provides a foundation for future expansions, such as windowing-based aggregations using Apache Beam.
4) Data Visualization: visualization.py retrieves historical metrics from the database and generates time-series plots for visual insights.

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

4) Run Visualization
    - This script generates time-series plots of key metrics from the database.
    python src/visualization.py


## Project Structure
Data-Engineer-Challenge/
├── src/                        
│   ├── data_stream.py         
│   ├── calculations.py         
│   ├── error_handling.py       
│   ├── visualization.py        
│   └── config.py               
├── tests/                      
│   ├── test_data_stream.py     
│   ├── test_calculations.py    
│   ├── test_visualization.py   
│   └── test_error_handling.py  
├── docs/                       
│   ├── assumptions.md          
│   ├── architecture.md         
│   ├── design_decisions.md     
│   └── architecture_diagram.png 
├── requirements.txt            
├── README.md                   
└── .env                        


## Testing
Unit tests are included for the main components (data_stream.py and calculations.py). To run tests, execute the following command:
   - PYTHONPATH=. python -m unittest discover tests
The tests include:
- Data Stream Tests: Verify that data is generated correctly in the expected format.
- Calculations Tests: Test that metrics are calculated accurately and database insertion is performed as expected.

## Assumptions
- A consistent data schema for cryptocurrency data.
- A pre-configured Kafka topic crypto_prices.
- A PostgreSQL table structure as outlined in the crypto_metrics table setup.
- A local development setup with Kafka and PostgreSQL accessible without complex network configurations.
See assumptions.md for a detailed list of assumptions.

## Design Decisions
The design of this project prioritizes:
- Real-time data processing: Using Kafka and Apache Beam for streaming and scalable data handling.
- Error handling and retries: Built-in retry mechanisms for handling transient errors.
- Modularity and extensibility: Separate scripts for data ingestion, processing, error handling, and visualization.
See design_decisions.md for a comprehensive overview.

## Future Improvements
- Enhanced Error Handling: Add more robust error handling and alerting mechanisms.
- Scalability: Use consumer groups to distribute data processing across multiple instances.
- Windowed Aggregations: Implement windowing in data_processing.py to support time-based aggregations.
- Advanced Visualization: Integrate with BI tools like Grafana or Tableau for dynamic, real-time data visualization.