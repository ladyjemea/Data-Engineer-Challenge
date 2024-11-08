# Assumptions made

## General
1) Data Consistency and Schema:
- The incoming data from Kafka has a consistent         structure, including fields such as pair, bid, ask, and timestamp.
- All required fields are always present in each message received from Kafka. Missing or malformed data will be handled by logging errors without crashing the program.

2) Currency Pairs:
- The system is designed to handle a limited set of cryptocurrency pairs (BTC-USD, ETH-USD, and LTC-USD) as per the current setup.
- It is assumed that adding or removing currency pairs will require updating the code to handle those pairs in data_ingestion.py and potentially in calculations.py.

3) Real-Time Data Processing:
- The system is expected to process data in real-time or near real-time, with a focus on providing up-to-date bid and ask metrics.
- A polling interval of 1 second for the Kafka consumer is adequate for the needs of this application.

## Kafka
1) Kafka Server:
- The Kafka server is running locally at localhost:9092 and is accessible without authentication.
- The topic crypto_prices is created beforehand in Kafka, and it is configured to retain messages for a sufficient duration to allow processing without message loss.

2) Message Ordering:
- Kafka provides messages in a reasonably ordered fashion, though exact ordering is not guaranteed.
- Minor reordering of messages does not impact the final metrics significantly.

3) Consumer Group Management:
- A single consumer instance (crypto_price_consumer_group) is used to process messages.
- If scaling up is required, additional consumers can be added to the group for parallel processing, though this version assumes only one active consumer.

## Database
1) Database Availability:
- The PostgreSQL database is running and accessible at the specified host, port, and credentials set in the environment variables.
- The database connection is persistent during the runtime of the consume_data process, and reconnection attempts are handled in case of transient connection issues.

2) Database Schema:
- The crypto_metrics table in the PostgreSQL database has been created in advance with the following fields:
    - pair: VARCHAR
    - bid: NUMERIC
    - ask: NUMERIC
    - mid_price: NUMERIC
    - spread: NUMERIC
    - highest_bid: NUMERIC
    - lowest_ask: NUMERIC
    - max_spread: NUMERIC
    - timestamp: TIMESTAMP
- The schema of this table will not change frequently. If additional fields are required, the schema and code will need to be updated accordingly

## Error Handling
1) Retry Logic:
- A retry mechanism is implemented for database connections and inserts, assuming that transient errors will resolve within a few retry attempts.
- n the event of an unrecoverable error (e.g., database is down for an extended period), errors are logged, and the program exits gracefully.

2) Logging:
- The logging configuration is sufficient to capture error messages, warnings, and informational messages related to data processing and storage.
- Logs provide enough detail for troubleshooting issues without overwhelming log storage.

## Data Processing
1) Metric Calculation:
- highest_bid, lowest_ask, and max_spread are cumulative metrics that are tracked across all messages received during runtime. This implies that the calculations start fresh each time the script is restarted.
- mid_price and spread are calculated on a per-message basis and stored in the database with each message processed.

2) Data Quality:
- The incoming data from Kafka is assumed to be accurate and within a reasonable range for cryptocurrency prices (e.g., bids and asks are positive, ask > bid).
- Outlier values are not handled explicitly, as it is assumed the data source (Coinbase market feeds) has built-in data quality controls.

## Environment Configuration Assumptions
1) Environment Variables:
- Sensitive information, such as database credentials, is stored in a .env file and loaded using the dotenv package.
- Environment variables are set correctly before the application starts, including DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, and Kafka configurations.

2) Virtual Environment:
- The application runs within a Python virtual environment, with all dependencies installed as specified in requirements.txt.