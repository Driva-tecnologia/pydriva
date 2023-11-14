import pytest
import pydriva
import os
import shutil
from dotenv import load_dotenv
load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

@pytest.fixture
def az():
    
    return pydriva.AzureStorage(conn_str=AZURE_STORAGE_CONNECTION_STRING)

def test_upload_file(az):
    # Create file
    os.makedirs('test_pydriva', exist_ok=True)
    with open('test_pydriva/test_file.txt', 'w') as f:
        f.write('This is a test file')
    
    # Upload file
    az.upload(
        container='tmp',
        src='test_pydriva/test_file.txt',
        dest='pydriva/test_file.txt'
    )

    # Remove the directory
    shutil.rmtree('test_pydriva')

    # Check if the file exists on the cloud
    assert az.exists('tmp', 'pydriva/test_file.txt')

    # Delete the file
    az.delete('tmp', 'pydriva/test_file.txt')
    az.delete('tmp', 'pydriva')

def test_upload_file_on_path(az):
    os.makedirs('test_pydriva', exist_ok=True)
    with open('test_pydriva/test_file.txt', 'w') as f:
        f.write('This is a test file')

    # Upload file
    az.upload(
        container='tmp',
        src='test_pydriva/test_file.txt',
        dest='pydriva/'
    )

    # Remove the directory
    shutil.rmtree('test_pydriva')
    
    # Check if the file exists on the cloud
    assert az.exists('tmp', 'pydriva/test_file.txt')

    # Delete the file
    az.delete('tmp', 'pydriva/test_file.txt')
    az.delete('tmp', 'pydriva')

def test_upload_recursive_directory(az):
    # Create a directory with a file inside
    os.makedirs('test_pydriva', exist_ok=True)
    os.makedirs('test_pydriva/test_dir', exist_ok=True)
    os.makedirs('test_pydriva/test_dir/test_dir2', exist_ok=True)
    with open('test_pydriva/test_dir/test_file.txt', 'w') as f: f.write('This is a test file')
    with open('test_pydriva/test_dir/test_file2.txt', 'w') as f: f.write('This is a test file')
    with open('test_pydriva/test_dir/test_dir2/test_file.txt', 'w') as f: f.write('This is a test file')
    
    
    # Upload directory
    az.upload(
        container='tmp',
        src='test_pydriva/test_dir',
        dest='pydriva'
    )

    # Remove the directory
    shutil.rmtree('test_pydriva')

    # Check if the file exists on the cloud
    assert az.exists('tmp', 'pydriva/test_dir')
    assert az.exists('tmp', 'pydriva/test_dir/test_file.txt')
    assert az.exists('tmp', 'pydriva/test_dir/test_file2.txt')

    # Delete the file
    az.delete('tmp', 'pydriva/test_dir/test_file.txt')
    az.delete('tmp', 'pydriva/test_dir/test_file2.txt')
    az.delete('tmp', 'pydriva/test_dir/test_dir2/test_file.txt')
    az.delete('tmp', 'pydriva/test_dir/test_dir2')
    az.delete('tmp', 'pydriva/test_dir')
    az.delete('tmp', 'pydriva')
