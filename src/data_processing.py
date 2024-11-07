"""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io import WriteToText
import json
import os
import logging

# Load environment variables from .env file or set defaults
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_prices")
OUTPUT_PATH = "processed_data/output.txt"  # Local or GCS path if using Dataflow

class CalculateMetrics(beam.DoFn):
    def process(self, element):
        '''Calculate metrics such as mid_price and spread.'''
        try:
            record = json.loads(element[1].decode('utf-8'))
            bid = record.get("bid")
            ask = record.get("ask")
            mid_price = (bid + ask) / 2
            spread = ask - bid
            record.update({"mid_price": mid_price, "spread": spread})
            yield json.dumps(record)
        except Exception as e:
            logging.error(f"Error processing record: {e}")

def run():
    print('starting data process')
    '''Main function to set up and execute the Beam pipeline.'''
    pipeline_options = PipelineOptions()
    
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read from Kafka" >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,
                    "group.id": "crypto_price_processor"
                },
                topics=[KAFKA_TOPIC]
            )
            | "Calculate Metrics" >> beam.ParDo(CalculateMetrics())
            | "Write to Output" >> WriteToText(OUTPUT_PATH)
        )

if __name__ == "__main__":
    run()
"""


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows

def run():
    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        # Simulated data input for local testing
        input_data = (
            p 
            | 'Create Sample Data' >> beam.Create([
                ('key1', 'value1'),
                ('key2', 'value2'),
                ('key1', 'value3'),
                ('key2', 'value4')
            ])
            | 'Apply Windowing' >> beam.WindowInto(FixedWindows(60))  # Apply a 60-second window for testing
            | 'Group by Key' >> beam.GroupByKey()
            | 'Print Results' >> beam.Map(print)  # Print to console for quick inspection
        )

if __name__ == '__main__':
    print("Starting data process...")
    run()