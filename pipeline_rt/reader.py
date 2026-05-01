import pandas as pd
import logging


class Reader:
    def __init__(self, file_path):
        self.file_path = file_path

    def read_file(self):
        try:
            df = pd.read_csv(self.file_path)
            logging.info(f"Successfully read {df.shape[0]:,} rows from {self.file_path}")
            return df
        except Exception as e:
            logging.error(f"Error reading file {self.file_path}: {e}")
            return None
