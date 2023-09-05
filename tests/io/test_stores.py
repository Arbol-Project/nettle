from unittest import TestCase
from unittest.mock import patch
from nettle.io.store import StoreInterface
from nettle.io.store import S3
from nettle.io.store import Local
from nettle.io.store import IPFS
import s3fs
import botocore
import os

s3_bucket_name = "arbol-station-dev"
credentials_name = "arbol-dev"

class StoreTestCase(TestCase):
    def setUp(self):
        with patch('nettle.utils.log_info.LogInfo') as MockClass:
            log = MockClass.return_value
        self.s3_store = S3(bucket=s3_bucket_name, credentials_name=credentials_name, log=log)
        self.local_store = Local(log=log)

    def test_name(self):
        self.assertEqual(self.s3_store.name(), 's3')
        self.assertEqual(self.local_store.name(), 'local')

    def test_deal_with_errors(self):
        with self.assertRaises(Exception):
            with self.local_store.deal_with_errors('/opt/test_folder'):
                raise Exception()


class S3StoreTestCase(TestCase):
    def setUp(self):
        with patch('nettle.utils.log_info.LogInfo') as MockClass:
            log = MockClass.return_value
        self.s3_store = S3(bucket=s3_bucket_name, credentials_name=credentials_name, log=log)

    def test_fs(self):
        self.assertTrue(self.s3_store.fs())
        self.assertTrue(isinstance(self.s3_store.fs(), s3fs.S3FileSystem))

    def test_inexistent_s3_profile(self):
        with self.assertRaises(botocore.exceptions.ProfileNotFound):
            S3()

    def test_list_directory_path_unfilled(self):
        with self.assertRaises(TypeError):
            self.s3_store.list_directory()

    def test_list_directory_bucket_non_existent(self):
        with self.assertRaises(FileNotFoundError):
            self.s3_store.list_directory(path='path_non_existent_d1b034')

    def test_list_directory(self):
        bucket_root = self.s3_store.list_directory(path=f"{self.s3_store.bucket}/")
        bucket_root_inexistent_folder = self.s3_store.list_directory(path=f"{self.s3_store.bucket}/path_non_existent_d1b034")
        self.assertTrue(len(bucket_root) > 0)
        self.assertTrue(len(bucket_root_inexistent_folder) == 0)

    def test_base_folder_without_s3(self):
        self.assertTrue(isinstance(self.s3_store.base_folder_without_s3, str))
        self.assertEqual(self.s3_store.base_folder_without_s3, f"{self.s3_store.bucket}/")

    def test_has_existing_file(self):
        self.assertFalse(self.s3_store.has_existing_file(f"path_non_existent_d1b034"))
        self.assertTrue(self.s3_store.has_existing_file(f"folder_for_test_dont_delete/metadata.json"))

    def test_has_existing_file_full_path(self):
        full_filepath_exists = os.path.join(
            self.s3_store.base_folder,
            f"folder_for_test_dont_delete/metadata.json"
        )
        full_filepath_dont_exist = os.path.join(
            self.s3_store.base_folder,
            f"path_non_existent_d1b034"
        )

        self.assertTrue(self.s3_store.has_existing_file_full_path(full_filepath_exists))
        self.assertFalse(self.s3_store.has_existing_file_full_path(full_filepath_dont_exist))

    # def test_cp_folder_to_remote(self):
    #     local_path = self.file_handler.PROCESSED_DATA_PATH
    #     relative_s3_path = os.path.dirname(self.file_handler.relative_path)
    #     self.s3_store.cp_folder_to_remote(local_path, relative_s3_path)
    #     pass

    # def test_write(self):
    #     self.s3_store.write()
    #     pass

    def test_read(self):
        content = self.s3_store.read(f"folder_for_test_dont_delete/metadata.json")
        content_non_existent = self.s3_store.read(f"path_non_existent_d1b034")
        self.assertTrue(content)
        self.assertFalse(content_non_existent)


# class LocalStoreTestCase(TestCase):
#     def setUp(self):
#         pass