
from azure.storage.filedatalake import (
    DataLakeServiceClient,
)
import os

class AzureStorage:
    def __init__(self, account_url, storage_account_key):
        self.service_client = DataLakeServiceClient(
            account_url=account_url,
            credential=storage_account_key
        )

    def get_directory_contents(self, container_name, directory_name):
        file_system_client = self.service_client.get_file_system_client(file_system=container_name)
        paths = file_system_client.get_paths(path=directory_name, recursive=False)
        
        return paths
    
    def download_directory(self, container_name, directory_name, local_path, overwrite=False) -> bool:
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        
        if not directory_name.endswith('/'):
            directory_name += '/'
        
        print(f'Downloading {directory_name} files to {local_path}')

        for path in self.get_directory_contents(container_name, directory_name):
            if path.is_directory:
                directory_name = path.name.replace(directory_name, '')

                self.download_directory(
                    container_name = container_name, 
                    directory_name = os.path.join(directory_name, directory_name), 
                    local_path = os.path.join(local_path, directory_name),
                    overwrite = overwrite
                )

            else:
                file_name = path.name.replace(directory_name, '')
                self.download_file(
                    container_name = container_name,
                    directory_name = directory_name,
                    file_name = file_name,
                    local_path = local_path,
                    overwrite = overwrite
                )
            

    def download_file(self, container_name, directory_name, file_name, local_path='.', overwrite=False) -> bool:
        file_system_client = self.service_client.get_file_system_client(file_system=container_name)
        directory_client = file_system_client.get_directory_client(directory_name)
        file_client = directory_client.get_file_client(file_name)

        if not os.path.exists(local_path):
            os.makedirs(local_path)
        
        if os.path.exists(os.path.join(local_path, file_name)) and not overwrite:
            print(f'File {file_name} already exists. Use overwrite=True to overwrite.')
            return False

        print(f'Downloading {file_name} to {local_path}')

        download = file_client.download_file()
        with open(os.path.join(local_path, file_name), "wb") as download_file:
            download_file.write(download.readall())

        return True

    def upload_file(self, container_name, directory_name, local_path, file_name, overwrite=False):
        file_system_client = self.service_client.get_file_system_client(file_system=container_name)
        directory_client = file_system_client.get_directory_client(directory_name)
        file_client = directory_client.get_file_client(file_name)

        print(f'Uploading {file_name} from {local_path} to {directory_name}')
        with open(file=os.path.join(local_path, file_name), mode="rb") as data:
            file_client.upload_data(data=data, overwrite=overwrite)