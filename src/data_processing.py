import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
import logging

# Configure logging to write to a file and display timestamps
logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_sample_data(pipeline):
    """
    Create a PCollection of sample key-value pairs.
    
    Parameters:
        pipeline (Pipeline): The Beam pipeline object.
    
    Returns:
        PCollection: A PCollection of key-value pairs.
    """
    return pipeline | 'Create Sample Data' >> beam.Create([
        ('key1', 'value1'),
        ('key2', 'value2'),
        ('key1', 'value3'),
        ('key2', 'value4')
    ])

def apply_windowing(input_data, window_size):
    """
    Apply fixed windowing to the input data.

    Parameters:
        input_data (PCollection): The input PCollection.
        window_size (int): Window size in seconds.

    Returns:
        PCollection: A PCollection with windowing applied.
    """
    return input_data | 'Apply Windowing' >> beam.WindowInto(FixedWindows(window_size))

def group_by_key(input_data):
    """
    Group input data by key.

    Parameters:
        input_data (PCollection): The input PCollection.

    Returns:
        PCollection: A PCollection grouped by key.
    """
    return input_data | 'Group by Key' >> beam.GroupByKey()

def log_results(input_data):
    """
    Log the processed results.

    Parameters:
        input_data (PCollection): The input PCollection.
    """
    return input_data | 'Log Results' >> beam.Map(lambda x: logging.info(f"Processed: {x}"))

def run_pipeline(window_size=60):
    """
    Define and execute the Apache Beam pipeline.

    Parameters:
        window_size (int): Window size in seconds for windowing.
    """
    # Configure pipeline options
    pipeline_options = PipelineOptions()

    # Define the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        sample_data = create_sample_data(p)
        windowed_data = apply_windowing(sample_data, window_size)
        grouped_data = group_by_key(windowed_data)
        log_results(grouped_data)

if __name__ == '__main__':
    logging.info("Starting data processing...")
    run_pipeline()