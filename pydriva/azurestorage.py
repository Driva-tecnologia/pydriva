
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobClient, BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
import os
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

class AzureStorage:
    def __init__(self, conn_str):
        self.credential = conn_str
        self.service_client = DataLakeServiceClient.from_connection_string(conn_str=conn_str)

    ########### PUBLIC METHODS ############

    def upload(self, container, src, dest):
        if not os.path.exists(src):
            raise Exception(f'{src} does not exists')
        elif os.path.isfile(src):
            return self._upload_file(container, src, dest)
        elif os.path.isdir(src):
            return self._upload_path(container, src, dest)
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
    def _upload_file(self, container, src, dest):
        filename = os.path.basename(src)

        if os.path.basename(dest) == '':
            blob_filename = os.path.join(dest, filename)
        else:
            blob_filename = dest
        
        blob = BlobClient.from_connection_string(conn_str=self.credential, container_name=container, blob_name=blob_filename)
        with open(src, "rb") as data:
            blob.upload_blob(data, overwrite=True)
        
        return True

    def _upload_path(self, container, src, dest):
        basename = os.path.basename(src)
        all_files = [file.name for file in Path(src).rglob('*') if file.is_file()]

        for file in tqdm(all_files, desc='Upload'):
            _src = os.path.join(src, file)
            _dest = os.path.join(dest, basename, file)
            
            self._upload_file(container, _src, _dest)

        return True

    ########### CHECK EXISTS ############
    def _exists(self, container, blob_name):
        return BlobClient.from_connection_string(conn_str=self.credential, container_name=container, blob_name=blob_name).exists()

    ########### DOWNLOAD ############
    def _download_file(self, container, src, dest):
        filename = os.path.join(dest, os.path.basename(src))
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        blob = BlobClient.from_connection_string(conn_str=self.credential, container_name=container, blob_name=src)
        blob.download_blob().readinto(open(filename, 'wb'))

        return True


    def _download(self, container, src, dest, log=True):
        dest = os.path.join(dest, os.path.basename(src))
        blobs = self._list_blobs(container, src)

        if not src.endswith('/'):
            src += '/'

        for blob in tqdm(blobs, desc='Download'):
            # remove the src from the blob name
            _src = blob.replace(src, '')
            _dest = os.path.join(dest, os.path.dirname(_src))
            self._download_file(container, blob, _dest)
            
        return True
            
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
