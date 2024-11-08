# Design Decisions

## Technology Stack Selection
1) Kafka for Data Ingestion
- Decision: Use Kafka as the message broker for streaming real-time cryptocurrency data.
- Rationale: Kafka is a highly reliable, fault-tolerant, and scalable distributed messaging system well-suited for real-time data ingestion and streaming. It supports high throughput and low latency, making it ideal for real-time data pipelines.
- Alternatives Considered: Alternatives such as Google Pub/Sub or Amazon Kinesis were considered. However, Kafka was chosen due to its popularity and support for on-premises deployments, which aligns with the local development setup in this challenge.

2) PostgreSQL for Data Storage
- Decision: Store processed metrics in a PostgreSQL database.
- Rationale: PostgreSQL is a robust, open-source relational database that supports SQL queries, ACID transactions, and data integrity. It is suitable for storing metrics in a structured format and can be easily queried for analysis or visualization.
- Alternatives Considered: NoSQL databases (e.g., MongoDB) were considered for flexibility, but PostgreSQL was selected to take advantage of SQL’s powerful querying capabilities for financial data analysis.

## Application Structure
1) Modular Code Organization
- Decision: Split the code into distinct modules (data_ingestion.py, data_stream.py, calculations.py, and data_processing.py) to separate concerns.
- Rationale: Modularizing the code improves readability, maintainability, and testability. Each module has a single responsibility, making it easier to troubleshoot, test, and extend in the future.
- Alternatives Considered: Combining all functionality into a single script would simplify the structure, but this would result in a less organized and more challenging codebase to maintain.

## Metrics Calculation
1) Highest Bid, Lowest Ask, and Max Spread Tracking
- Decision: Track the highest bid, lowest ask, and maximum spread as cumulative metrics across all incoming data.
- Rationale: These metrics provide meaningful insights into market volatility and allow the finance team to understand price variations over time. By tracking these metrics continuously, we can maintain a real-time view of the market without relying on batch processing.
- Alternatives Considered: Storing all bid/ask values and calculating metrics periodically was considered but would introduce higher storage costs and latency.

2) Real-Time Mid-Price Calculation
- Decision: Calculate the mid-price for each message as the average of the bid and ask prices.
- Rationale: Mid-price provides an approximate value for the market price, which is useful for monitoring price trends. Calculating it in real-time allows the finance team to track changes without delay.
- Alternatives Considered: Aggregating mid-prices over a windowed period (e.g., minute-based) was considered, but a per-message calculation provides immediate updates that better align with the real-time requirements.

## Error Handling and Resilience
1) Retry Mechanism for Database and Kafka Operations
- Decision: Implement a retry mechanism for database and Kafka interactions.
- Rationale: Transient errors, such as network interruptions or temporary service unavailability, should not cause the application to crash. The retry mechanism ensures the application remains robust and handles such intermittent issues gracefully.
- Alternatives Considered: Simple error logging without retry logic was considered, but this would result in data loss or incomplete processing if errors occurred frequently.

2) Logging Configuration
- Decision: Use structured logging with log levels (INFO, ERROR) for monitoring and troubleshooting.
- Rationale: Logging provides a record of application behavior, which is essential for debugging and operational monitoring. By including log levels and structured messages, it's easier to filter and analyze logs.
- Alternatives Considered: Only logging errors was considered to reduce log volume, but detailed logs provide valuable information for understanding normal operations and diagnosing issues.

## Environment Configuration and Security
1) Use of Environment Variables and .env File
- Decision: Load sensitive configuration values (e.g., database credentials, Kafka servers) from environment variables set in a .env file.
- Rationale: This approach ensures that sensitive information is not hard-coded in the codebase, enhancing security and allowing for different configurations in development and production environments.
- Alternatives Considered: Hardcoding configuration values in the scripts was considered simpler but would risk exposing sensitive information and reduce flexibility.

## Testing Strategy
1) Unit Testing with Mocks
- Decision: Use the unittest library with mock objects for testing components that interact with external services (e.g., Kafka and PostgreSQL).
- Rationale: Mocking allows us to test each function in isolation without relying on an actual Kafka server or database, making tests faster and more reliable.
- Alternatives Considered: End-to-end testing was considered but would require complex setup and teardown procedures, and may not be feasible in a local or CI/CD environment.

2) Assumptions on Data Consistency and Format
- Decision: Assume the structure and format of incoming data from Kafka is consistent and validate data before processing.
- Rationale: Handling unexpected data formats or missing fields could result in application errors or incorrect metrics. By making this assumption, we can simplify error handling and focus on meaningful data processing.
- Alternatives Considered: Implementing strict data validation would provide higher resilience but would add complexity to the system and potentially slow down real-time processing.

## Future Scalability Considerations
1) Consumer Group for Kafka Scaling
- Decision: Configure the Kafka consumer with a consumer group (crypto_price_consumer_group), enabling horizontal scaling by adding more consumers if needed.
- Rationale: Using a consumer group allows multiple consumers to process data from the same topic in parallel, supporting scalability as data volume increases.
- Alternatives Considered: Single consumer instances would limit scalability and slow down data processing if the message volume significantly increased.

2) Partitioning and Windowing for Aggregation
- Decision: Use real-time processing and windowing for future enhancements to aggregate metrics over defined intervals (e.g., 1-minute windows).
- Rationale: Windowing can help in summarizing data over short timeframes and could allow for additional insights and trend analysis without storing every message.
- Alternatives Considered: Storing all records for offline batch aggregation would be storage-intensive and reduce the real-time nature of the application.
