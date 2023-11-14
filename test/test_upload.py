import pydriva
import os
from dotenv import load_dotenv

load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

az = pydriva.AzureStorage(conn_str=AZURE_STORAGE_CONNECTION_STRING)

az.upload(
    container='tmp', 
    local_path='example/metadados00054.parquet',
    path='test_pydriva/'
)