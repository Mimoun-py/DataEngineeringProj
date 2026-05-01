import os
import io
import logging
from datetime import datetime

import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()


class Writer:

    def __init__(self):
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        container_name    = os.getenv('AZURE_CONTAINER_NAME')

        if not connection_string or not container_name:
            logging.warning("Azure credentials not found in environment. Azure upload will be skipped.")
            self.container_client = None
        else:
            service_client        = BlobServiceClient.from_connection_string(connection_string)
            self.container_client = service_client.get_container_client(container_name)

    # ── local ─────────────────────────────────────────────────────────────────

    def write_local(self, df: pd.DataFrame, output_dir: str, filename: str) -> str:
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, filename)
        df.to_csv(file_path, index=False)
        logging.info(f"Written locally: {file_path} ({df.shape[0]} rows)")
        return file_path

    # ── azure ─────────────────────────────────────────────────────────────────

    def write_to_blob(self, df: pd.DataFrame, blob_name: str):
        if self.container_client is None:
            logging.warning("Skipping Azure upload — no credentials configured.")
            return

        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        self.container_client.upload_blob(name=blob_name, data=buffer, overwrite=True)
        logging.info(f"Uploaded to Azure Blob: {blob_name}")

    # ── combined entry point ──────────────────────────────────────────────────

    def write(self, df: pd.DataFrame, output_dir: str, blob_name: str):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename  = f"ecommerce_processed_{timestamp}.csv"

        self.write_local(df, output_dir, filename)
        self.write_to_blob(df, blob_name)
