from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import logging
import os
import io


class Writer:
    def __init__(self):
        load_dotenv()
        self.connection_string = os.getenv('AZURE_CONNECTION_STRING')
        self.container_name = os.getenv('AZURE_CONTAINER_NAME')
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)

    def write_to_blob(self, df, blob_name):
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        self.container_client.upload_blob(name=blob_name, data=buffer, overwrite=True)
        logging.info(f"Written to Azure Blob Storage as {blob_name}")

    def write_local(self, df, file_path):
        df.to_parquet(file_path, index=False)
        logging.info(f"Written locally to {file_path}")


    def write(self, df, file_path, blob_name):
        try:
            self.write_local(df, file_path)
            self.write_to_blob(df, blob_name)

        except Exception as e:
            logging.error(f"Error occurred while writing data: {e}")
