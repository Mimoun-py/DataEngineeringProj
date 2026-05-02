import sys
import logging

# Make pipeline modules importable when called from Airflow
sys.path.insert(0, '/opt/airflow/pipeline')

from reader import Reader
from validator import Validator
from processor import Processor
from writer import Writer

# Paths inside the Docker container (mounted via docker-compose volumes)
INPUT_PATH  = '/opt/airflow/data/yellow_tripdata_2025-01.parquet'
OUTPUT_PATH = '/opt/airflow/output/yellow_tripdata_processed_2025-01.parquet'
BLOB_NAME   = 'yellow-taxi/yellow_taxi_processed_2025-01.parquet'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


def run_pipeline():
    # Step 1: Read
    reader = Reader(INPUT_PATH)
    df = reader.read_file()
    if df is None:
        raise ValueError(f"Reading failed. File not found or unreadable: {INPUT_PATH}")

    # Step 2: Validate
    validator = Validator()
    df, report = validator.validate(df)

    # Step 3: Process
    processor = Processor()
    df = processor.process(df)

    # Step 4: Write locally and to Azure Blob Storage
    writer = Writer()
    writer.write(df, OUTPUT_PATH, BLOB_NAME)
    logging.info("Pipeline complete!")


if __name__ == "__main__":
    run_pipeline()
