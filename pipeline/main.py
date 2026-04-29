import os
from reader import Reader
from validator import Validator
from processor import Processor
from writer import Writer
import logging
from datetime import datetime

os.makedirs('logs', exist_ok=True)

logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
        , handlers=[
            logging.FileHandler(f"logs/pipeline{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log", mode='a'),
            logging.StreamHandler()
        ] 
)

def main():
    # Step 1: Read the data
    file_path = '../data/yellow_tripdata_2025-01.parquet'
    reader = Reader(file_path)
    df = reader.read_file()
    if df is None:
        logging.error("Data reading failed. Exiting pipeline.")
        return
    
    # Step 2: Validate the data and log any issues
    validator = Validator()
    df, report = validator.validate(df)
    logging.info(f"Validation report: {report}")
    
    # Step 3: Process the data
    processor = Processor()
    df = processor.process(df)
    logging.info("Data processing complete.")

    # Step 4: Write the processed data to Azure Blob Storage and locally
    writer = Writer()
    writer.write(df, file_path="../data/output/processed_data.parquet", blob_name="processed_data.parquet")
    logging.info("Data writing complete.")

if __name__ == "__main__":
    main()
    