from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import logging
import os
import io


class Writer:
    def __init__(self):
        load_dotenv()
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        container_name    = os.getenv('AZURE_CONTAINER_NAME')

        if not connection_string or not container_name:
            logging.warning("Azure credentials not found in environment. Azure upload will be skipped.")
            self.container_client = None
        else:
            service_client        = BlobServiceClient.from_connection_string(connection_string)
            self.container_client = service_client.get_container_client(container_name)

    def write_to_blob(self, df, blob_name):
        if self.container_client is None:
            logging.warning("Skipping Azure upload — no credentials configured.")
            return

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        self.container_client.upload_blob(name=blob_name, data=buffer, overwrite=True)
        logging.info(f"Written to Azure Blob Storage as {blob_name}")

    def write_local(self, df, file_path):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df.to_parquet(file_path, index=False)
        logging.info(f"Written locally to {file_path}")

    def write(self, df, file_path, blob_name):
        self.write_local(df, file_path)
        self.write_to_blob(df, blob_name)
