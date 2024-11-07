import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
import logging

logging.basicConfig(filename='pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run():
    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        input_data = (
            p 
            | 'Create Sample Data' >> beam.Create([
                ('key1', 'value1'),
                ('key2', 'value2'),
                ('key1', 'value3'),
                ('key2', 'value4')
            ])
            | 'Apply Windowing' >> beam.WindowInto(FixedWindows(60))
            | 'Group by Key' >> beam.GroupByKey()
            | 'Log Results' >> beam.Map(lambda x: logging.info(f"Processed: {x}"))
        )

if __name__ == '__main__':
    logging.info("Starting data processing...")
    run()