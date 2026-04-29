import pandas as pd
import logging 

class Reader:
    def __init__(self, file_path):
        self.file_path = file_path

    def read_file(self):
        try:
            df = pd.read_parquet(self.file_path, engine='pyarrow')
            logging.info(f"Successfully read file with {df.shape[0]} rows and {df.shape[1]} columns from {self.file_path}.")
            return df
        except Exception as e:
            logging.error(f"Error reading file {self.file_path}: {e}")
            return None
        
    