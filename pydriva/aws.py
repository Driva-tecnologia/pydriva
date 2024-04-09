import boto3
import os
from cloudpathlib import CloudPath, S3Client
from botocore.exceptions import ClientError

class AwsS3:
    def __init__(self, access_key, secret_key):
        self.aws = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='bhs',
            endpoint_url='https://s3.bhs.io.cloud.ovh.net/'
        )
        self.cp_client = S3Client(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url='https://s3.bhs.io.cloud.ovh.net/'
        )
        # Test Connection
        try:
            self.aws.head_bucket(Bucket="drivalake")
        except ClientError as e:
            raise Exception(f"Invalid credentials or insufficient access. ClientError: {e}")

    ########### PUBLIC METHODS ############

    def upload(self, container, src, dest):
        if not os.path.exists(src):
            raise Exception(f'{src} does not exist.')
        elif os.path.isfile(src):
            return self._upload(container, src, dest)
        elif os.path.isdir(src):
            return self._upload(container, src, dest)
        raise Exception(f'{src} is not a file or a directory')

    def exists(self, container, path):
        return self._exists(container, path)

    def download(self, container, src, dest="."):
        if not self.exists(container, src):
            raise Exception(f's3://{container}/{src} does not exist.')
        return self._download(container, src, dest)
    
    def delete(self, container, path):
        if self.exists(container, path):
            return self._delete(container, path)
        return False
    
    def list(self, container, path):
        return self._list(container, path)

    ########### PRIVATE METHODS ############

    def _upload(self, container, src, dest):
        try:
            self.aws.upload_file(src, container, dest)
            return True
        except:
            return False

    def _exists(self, container, path):
        try:
            # If object exists, returns true
            self.aws.get_object(Bucket=container, Key=path)
            return True
        except:
            # If folder exists, returns true
            path = path.rstrip("/")
            response = self.aws.list_objects(Bucket=container, Prefix=path, Delimiter="/", MaxKeys=1)
            if 'CommonPrefixes' not in response:
                return False
            return path == response['CommonPrefixes'][0]['Prefix'].rstrip("/")
        
    def _download(self, container, src, dest):
        cp = CloudPath(f"s3://{container}/{src}", client=self.cp_client)
        # Download Folder
        if cp.is_dir():
            cp.download_to(dest)
            return True
        # Download File
        if os.path.isdir(dest):
            dest = os.path.join(dest, os.path.basename(src))
        response = self.aws.download_file(container, src, dest)
        return True
        
    def _delete(self, container, path):
        cp = CloudPath(f"s3://{container}/{path}", client=self.cp_client)
        # Delete Folder
        if cp.is_dir():
            cp.rmtree()
            return True
        # Delete File
        self.aws.delete_object(Bucket=container, Key=path)
        return True

    def _list(self, container, path):
        response = self.aws.list_objects(Bucket=container, Prefix=path, Delimiter="/")
        folders = []
        files = []
        if 'CommonPrefixes' in response:
            folders = [folder['Prefix'] for folder in response['CommonPrefixes']]
        if 'Contents' in response:
            files = [file['Key'] for file in response["Contents"]]
        return folders + files