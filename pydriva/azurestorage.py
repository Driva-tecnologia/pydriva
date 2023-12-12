
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobClient, BlobServiceClient
import os
from tqdm import tqdm
from pathlib import Path
from azcopy_wrapper.azcopy_client import AzClient
from azcopy_wrapper.azcopy_utilities import AzCopyOptions

class AzureStorage:
    def __init__(self, conn_str):
        self.credential = conn_str
        self.service_client = DataLakeServiceClient.from_connection_string(conn_str=conn_str)
        self.azcopy = AzClient()

        self.storage_account_name = conn_str.split(';')[1].split('=')[1]

    ########### PUBLIC METHODS ############

    def upload(self, container, src, dest):
        return self._upload(container, src, dest)
        
    def exists(self, container, blob_name):
        return self._exists(container, blob_name)

    def download(self, container, src, dest):
        return self._download(container, src, dest)
    
    def delete(self, container, path):
        if self.exists(container, path):
            return self._delete(container, path)
        return False
    
    def list(self, container, path):
        return self._list(container, path)


    ########### PRIVATE METHODS ############

    ########### UPLOAD ############
    def _upload(self, container, src, dest):
        dest = f'https://{self.storage_account_name}.blob.core.windows.net/{container}/{dest}'
        try:
            self.azcopy.upload_data_to_remote_location(
                src=src,
                dest=dest,
                transfer_options= AzCopyOptions(
                    recursive=True,
                    overwrite_existing=True
                ))
            return True
        except Exception as e:
            print(f'Login AzCopy!')
            return False
    ########### CHECK EXISTS ############
    def _exists(self, container, blob_name):
        return BlobClient.from_connection_string(conn_str=self.credential, container_name=container, blob_name=blob_name).exists()

    ########### DOWNLOAD ############
    def _download(self, container, src, dest):
        src = f'https://{self.storage_account_name}.blob.core.windows.net/{container}/{src}'

        try:
            self.azcopy.download_data_to_local_location(
                src=src,
                dest=dest,
                transfer_options= AzCopyOptions(
                    recursive=True,
                    overwrite_existing=True
                ))
            return True
        except Exception as e:
            print(f'Login AzCopy!')
            return False
        
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