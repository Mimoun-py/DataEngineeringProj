import sys
import os
import logging

# Make pipeline modules importable when called from Airflow
sys.path.insert(0, '/opt/airflow/pipeline_rt')

from reader import Reader
from validator import Validator
from processor import Processor
from writer import Writer

# Paths inside the Docker container (mounted via docker-compose volumes)
OUTPUT_DIR = '/opt/airflow/output'
BLOB_NAME  = 'ecommerce/ecommerce_processed.csv'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


def run_pipeline(file_path: str = None):
    if file_path is None:
        raise ValueError("No file path provided to run_pipeline.")

    logging.info(f"Starting real-time pipeline for: {file_path}")

    # Step 1: Read
    reader = Reader(file_path)
    df = reader.read_file()
    if df is None:
        raise ValueError(f"Reading failed. File not found or unreadable: {file_path}")

    # Step 2: Validate
    validator = Validator()
    df, report = validator.validate(df)

    # Step 3: Process
    processor = Processor()
    df = processor.process(df)

    # Step 4: Write locally and to Azure Blob Storage
    writer = Writer()
    writer.write(df, output_dir=OUTPUT_DIR, blob_name=BLOB_NAME)

    logging.info("Pipeline complete!")


if __name__ == '__main__':
    run_pipeline('../input/ecommerce_orders.csv')
