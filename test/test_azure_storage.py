import unittest
import pydriva
import os
import shutil

from dotenv import load_dotenv

load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

class TestUpload(unittest.TestCase):
    az = pydriva.AzureStorage(conn_str=AZURE_STORAGE_CONNECTION_STRING)

    def test_upload_file(self):
        local_path = 'test_file.txt'
        open(local_path, 'w').close()
        try:
            worked = self.az.upload(
                container='tmp', 
                path='test_pydriva/test_file.txt',
                local_path='test_file.txt'
            )
        except Exception as e:
            worked = False

        if os.path.exists(local_path):
            os.remove(local_path)

        self.assertTrue(worked)

    def test_upload_file_on_path(self):
        local_path = 'test_file_on_path.txt'
        open(local_path, 'w').close()
        try:
            worked = self.az.upload(
                container='tmp', 
                path='test_pydriva/',
                local_path='test_file_on_path.txt'
            )
        except Exception as e:
            worked = False

        if os.path.exists(local_path):
            os.remove(local_path)

        self.assertTrue(worked)

    def test_upload_path(self):
        os.makedirs('test_dir', exist_ok=True)
        open('test_dir/test_file.txt', 'w').close()

        try:
            worked = self.az.upload(
                container='tmp', 
                path='test_pydriva/test_dir',
                local_path='test_dir'
            )
        except Exception as e:
            worked = False
        
        if os.path.exists('test_dir'):
            shutil.rmtree('test_dir')
        
        self.assertTrue(worked)

    def test_upload_path_with_subdirectories(self):
        os.makedirs('test_dir_sub', exist_ok=True)
        os.makedirs('test_dir_sub/test_subdir', exist_ok=True)
        open('test_dir_sub/test_file.txt', 'w').close()
        open('test_dir_sub/test_subdir/test_inside_file.txt', 'w').close()

        try:
            worked = self.az.upload(
                container='tmp', 
                path='test_pydriva/test_dir_sub',
                local_path='test_dir_sub'
            )
        except Exception as e:
            worked = False

        if os.path.exists('test_dir_sub'):
            shutil.rmtree('test_dir_sub')
        
        self.assertTrue(worked)

class TestCheckExists(unittest.TestCase):
    az = pydriva.AzureStorage(conn_str=AZURE_STORAGE_CONNECTION_STRING)

    def test_check_exists_file(self):
        self.assertTrue(
            self.az.check_exists(
                container='tmp', 
                path='test_pydriva/test_file.txt'
            )
        )
    
    def test_check_exists_path(self):
        self.assertTrue(
            self.az.check_exists(
                container='tmp', 
                path='test_pydriva'
            )
        )
    
    def test_check_exists_subdirectory(self):
        self.assertTrue(
            self.az.check_exists(
                container='tmp', 
                path='test_pydriva/test_dir_sub'
            )
        )
    
    def test_check_exists_subdirectory_with_file(self):
        self.assertTrue(
            self.az.check_exists(
                container='tmp', 
                path='test_pydriva/test_dir_sub/test_file.txt'
            )
        )

    def test_check_not_exists_file(self):
        self.assertFalse(
            self.az.check_exists(
                container='tmp', 
                path='test_pydriva/randomname.txt'
            )
        )



class TestDownload(unittest.TestCase):
    az = pydriva.AzureStorage(conn_str=AZURE_STORAGE_CONNECTION_STRING)

    def test_download_file(self):
        self.az.download(
            container='tmp', 
            path='test_pydriva/test_file.txt',
            local_path='test_pydriva'
        )
        file_exists = os.path.exists('test_pydriva/test_file.txt')
        if file_exists:
            shutil.rmtree('test_pydriva')
        self.assertTrue(file_exists)

    def test_download_path(self):
        self.az.download(
            container='tmp', 
            path='test_pydriva/test_dir',
            local_path='test_pydriva'
        )
        file_exists = os.path.exists('test_pydriva/test_dir')
        if file_exists:
            shutil.rmtree('test_pydriva')

        self.assertTrue(file_exists)

    def test_donwload_path_with_sub(self):
        self.az.download(
            container='tmp', 
            path='test_pydriva/test_dir_sub',
            local_path='test_pydriva'
        )
        file_exists = os.path.exists('test_pydriva/test_dir_sub')
        if file_exists:
            shutil.rmtree('test_pydriva')

        self.assertTrue(file_exists)

class TestDelete(unittest.TestCase):
    az = pydriva.AzureStorage(conn_str=AZURE_STORAGE_CONNECTION_STRING)

    def test_delete_file(self):
        self.assertTrue(
            self.az.delete(
                container='tmp', 
                path='test_pydriva/test_file.txt'
            )
        )
    def test_delete_second(self):
        self.assertTrue(
            self.az.delete(
                container='tmp', 
                path='test_pydriva/test_file_on_path.txt'
            )
        )
    def test_delete_path(self):
        self.assertTrue(
            self.az.delete(
                container='tmp', 
                path='test_pydriva/test_dir'
            )
        )
    def test_delete_path_with_sub(self):
        self.assertTrue(
            self.az.delete(
                container='tmp', 
                path='test_pydriva/test_dir_sub'
            )
        )
    def test_delete_test_path(self):
        self.assertTrue(
            self.az.delete(
                container='tmp', 
                path='test_pydriva'
            )
        )

if __name__ == '__main__':
    unittest.main()