
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient
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

    def upload(self, container, path, local_path):
        if not os.path.exists(local_path):
            raise Exception(f'{local_path} does not exists')
        elif os.path.isfile(local_path):
            return self._upload_file(container, path, local_path)
        elif os.path.isdir(local_path):
            return self._upload_path(container, path, local_path)
        raise Exception(f'{local_path} is not a file or a directory')
        
    def check_exists(self, container, path):
        return self._check_exists(container, path)

    def download(self, container, path, local_path='.', log=True):
        local_path = os.path.join(local_path, os.path.basename(path))

        return self._download_file(container, path, local_path, log)
        # return self._download_path(container, path, log)
    
    def delete(self, container, path):
        if self._check_exists(container, path):
            return self._delete(container, path)
        return False
    

    ########### PRIVATE METHODS ############

    ########### UPLOAD ############
    def _upload_file(self, container, path, local_path):
        filename = os.path.basename(local_path)

        if os.path.basename(path) == '':
            blob_filename = os.path.join(path, filename)
        else:
            blob_filename = path
        
        blob = BlobClient.from_connection_string(conn_str=self.credential, container_name=container, blob_name=blob_filename)
        with open(local_path, "rb") as data:
            blob.upload_blob(data, overwrite=True)
        
        return True

    def _upload_path(self, container, path, local_path):
        all_files = [file for file in Path(local_path).rglob('*') if file.is_file()]

        for file in tqdm(all_files, desc='Upload'):
            dirname = os.path.dirname(file) 
            dst = os.path.join(path, dirname)+os.sep
            self._upload_file(container, dst, file)
        return True

    ########### CHECK EXISTS ############
    def _check_exists(self, container, path):
        file_system_client = self.service_client.get_file_system_client(file_system=container)
        
        exists = file_system_client.get_file_client(path).exists()
        return exists

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
        file_system_client = self.service_client.get_file_system_client(file_system=container)
        file_client = file_system_client.get_file_client(path)
        directory_client = file_system_client.get_directory_client(path)

        try:
            directory_client.delete_directory()
        except ResourceNotFoundError:
            print(f'Path {path} not found')
            raise ResourceNotFoundError(f'Path {path} not found')
        return True

    ########### OTHERS ############
    

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    az = AzureStorage(conn_str=AZURE_STORAGE_CONNECTION_STRING)
    az.upload(
        container='tmp', 
        path='test_pydriva/',
        local_path='test_dir_sub'
    )