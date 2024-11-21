import logging

# Configure the logging system
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO (logs INFO, WARNING, ERROR, and CRITICAL levels)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log message format
    datefmt='%Y-%m-%d %H:%M:%S',  # Set the timestamp format to "Year-Month-Day Hour:Minute:Second"
    handlers=[
        logging.FileHandler("pipeline.log"),  # Write log messages to a file named "pipeline.log"
        logging.StreamHandler()  # Output log messages to the console
    ]
)
