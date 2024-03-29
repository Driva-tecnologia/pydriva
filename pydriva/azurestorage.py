
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobClient, BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
import os
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from azcopy_wrapper.azcopy_client import AzClient
from azcopy_wrapper.azcopy_client import AzCopyOptions

class AzureStorage:
    def __init__(self, conn_str):
        self.credential = conn_str
        self.service_client = DataLakeServiceClient.from_connection_string(conn_str=conn_str)
        self.az = AzClient()

    ########### PUBLIC METHODS ############

    def upload(self, container, src, dest):
        if not os.path.exists(src):
            raise Exception(f'{src} does not exists')
        elif os.path.isfile(src):
            return self._upload(container, src, dest)
        elif os.path.isdir(src):
            return self._upload(container, src, dest)
        
        raise Exception(f'{src} is not a file or a directory')
        
    def exists(self, container, blob_name):
        return self._exists(container, blob_name)

    def download(self, container, src, dest):
        return self._download(container, src, dest)
        # return self._download_path(container, path, log)
    
    def delete(self, container, path):
        if self.exists(container, path):
            return self._delete(container, path)
        return False
    
    def list(self, container, path):
        return self._list(container, path)


    ########### PRIVATE METHODS ############

    ########### UPLOAD ############
    def _upload(self, container, src, dest):
        transfer_option = AzCopyOptions(overwrite_existing=True, recursive=True)
        job_info = self.az.upload_data_to_remote_location(src=src, dest=dest, transfer_options=transfer_option)
        
        return job_info.__dict__['completed']

    ########### CHECK EXISTS ############
    def _exists(self, container, blob_name):
        return BlobClient.from_connection_string(conn_str=self.credential, container_name=container, blob_name=blob_name).exists()

    ########### DOWNLOAD ############
    def _download(self, container, src, dest):
        transfer_option = AzCopyOptions(overwrite_existing=True, recursive=True)
        job_info = self.az.download_data_to_local_location(src=src, dest=dest, transfer_options=transfer_option)
        return job_info.__dict__['completed']

    ########### DELETE ############
    def _delete(self, container, path):
        blob = BlobClient.from_connection_string(conn_str=self.credential, container_name=container, blob_name=path)
        blob.delete_blob()
        
        return True

    ########### LIST ############
    def _list(self, container, path):
        blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.credential)
        container_client = blob_service_client.get_container_client(container)
        blobs = container_client.list_blobs(name_starts_with=path)

        return [blob.name for blob in blobs]

    def _list_blobs(self, container, path):
        blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.credential)
        container_client = blob_service_client.get_container_client(container)
        blobs = container_client.list_blob_names(name_starts_with=path)

        return [blob for blob in blobs if '.' in blob]
    ########### OTHERS ############


if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    conn_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    az = AzureStorage(conn_str=conn_str)

    az.download(
        container='tmp',
        src='pydriva/test_dir',
        dest='test_pydriva'
    )
