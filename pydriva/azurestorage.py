
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

    def download(self, container, path, local_path='.', log=True):
        local_path = os.path.join(local_path, os.path.basename(path))

        return self._download_file(container, path, local_path, log)
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
    def _download_file(self, container, path, local_path, log=True):
        file_system_client = self.service_client.get_file_system_client(file_system=container)


        if not file_system_client.get_file_client(path).exists():
            raise ResourceNotFoundError(f'{path} does not exists')

        blob_list = [blob.name for blob in file_system_client.get_paths(path=path, recursive=True) if blob.is_directory == False]

        for blob in tqdm(blob_list, desc='Download'):
            blob_dir = os.path.dirname(blob).replace(path, '', 1).replace('/', '', 1)
            local_dir = os.path.join(local_path, blob_dir)
            if local_dir != '':
                os.makedirs(local_dir, exist_ok=True)
            file_client = file_system_client.get_file_client(blob)
            download = file_client.download_file()

            filename = os.path.join(local_dir, os.path.basename(blob))
            with open(filename, "wb") as download_file:
                download_file.write(download.readall())
        
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

    ########### OTHERS ############
