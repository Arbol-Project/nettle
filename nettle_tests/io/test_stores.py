from unittest import TestCase
from unittest.mock import patch
from nettle.io.store import S3
from nettle.io.store import Local
import s3fs
import botocore
import os
import fsspec
import pandas as pd

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


# class S3StoreTestCase(TestCase):
#     def setUp(self):
#         with patch('nettle.utils.log_info.LogInfo') as MockClass:
#             log = MockClass.return_value
#         self.s3_store = S3(bucket=s3_bucket_name, credentials_name=credentials_name, log=log)
#
#     def test_fs(self):
#         self.assertTrue(self.s3_store.fs())
#         self.assertTrue(isinstance(self.s3_store.fs(), s3fs.S3FileSystem))
#
#     def test_inexistent_s3_profile(self):
#         with self.assertRaises(botocore.exceptions.ProfileNotFound):
#             S3()
#
#     def test_list_directory_path_unfilled(self):
#         with self.assertRaises(TypeError):
#             self.s3_store.list_directory()
#
#     def test_list_directory_bucket_non_existent(self):
#         with self.assertRaises(FileNotFoundError):
#             self.s3_store.list_directory(path='path_non_existent_d1b034')
#
#     def test_list_directory(self):
#         bucket_root = self.s3_store.list_directory(path=f"{self.s3_store.bucket}/")
#         bucket_root_inexistent_folder = self.s3_store.list_directory(path=f"{self.s3_store.bucket}/path_non_existent_d1b034")
#         self.assertTrue(len(bucket_root) > 0)
#         self.assertTrue(len(bucket_root_inexistent_folder) == 0)
#
#     def test_base_folder_without_s3(self):
#         self.assertTrue(isinstance(self.s3_store.base_folder_without_s3, str))
#         self.assertEqual(self.s3_store.base_folder_without_s3, f"{self.s3_store.bucket}/")
#
#     def test_has_existing_file(self):
#         self.assertFalse(self.s3_store.has_existing_file(f"path_non_existent_d1b034"))
#         self.assertTrue(self.s3_store.has_existing_file(f"folder_for_test_dont_delete/metadata.json"))
#
#     def test_has_existing_file_full_path(self):
#         full_filepath_exists = os.path.join(
#             self.s3_store.base_folder,
#             f"folder_for_test_dont_delete/metadata.json"
#         )
#         full_filepath_dont_exist = os.path.join(
#             self.s3_store.base_folder,
#             f"path_non_existent_d1b034"
#         )
#
#         self.assertTrue(self.s3_store.has_existing_file_full_path(full_filepath_exists))
#         self.assertFalse(self.s3_store.has_existing_file_full_path(full_filepath_dont_exist))
#
#     # def test_cp_folder_to_remote(self):
#     #     local_path = self.file_handler.PROCESSED_DATA_PATH
#     #     relative_s3_path = os.path.dirname(self.file_handler.relative_path)
#     #     self.s3_store.cp_folder_to_remote(local_path, relative_s3_path)
#     #     pass
#
#     def test_write_none_json(self):
#         with self.assertRaises(Exception) as cm:
#             self.s3_store.write(f"folder_for_test_dont_delete/test_metadata_none.json", None)
#         self.assertEqual(
#             "[store.write] content file not identified",
#             str(cm.exception)
#         )
#
#     def test_write_none_csv(self):
#         with self.assertRaises(Exception) as cm:
#             self.s3_store.write(f"folder_for_test_dont_delete/test_metadata_none.csv", None)
#         self.assertEqual(
#             "[store.write] content file not identified",
#             str(cm.exception)
#         )
#
#     def test_write_without_termination(self):
#         content = {"a": 1, "b": 2}
#         filepath = self.s3_store.write(f"folder_for_test_dont_delete/test_metadata_without_termination", content)
#         self.assertEqual(filepath, f"folder_for_test_dont_delete/test_metadata_without_termination")
#
#     def test_write_json(self):
#         content = {"a": 1, "b": 2}
#         filepath = self.s3_store.write(f"folder_for_test_dont_delete/test_metadata.json", content)
#         self.assertEqual(filepath, f"folder_for_test_dont_delete/test_metadata.json")
#
#     # def test_write_csv(self):
#     #     content = ''
#     #     self.s3_store.write(f"folder_for_test_dont_delete/KALUMBRU.csv", content)
#
#     def test_read_without_termination(self):
#         content_non_existent = self.s3_store.read(f"metadata")
#         self.assertFalse(content_non_existent)
#
#     def test_read_json_non_existent(self):
#         content_non_existent = self.s3_store.read(f"path_non_existent_d1b034.json")
#         self.assertFalse(content_non_existent)
#
#     def test_read_csv_non_existent(self):
#         content_non_existent = self.s3_store.read(f"path_non_existent_d1b034.csv")
#         self.assertFalse(content_non_existent)
#
#     def test_read_json(self):
#         content = self.s3_store.read(f"folder_for_test_dont_delete/metadata.json")
#         self.assertTrue(content)
#
#     def test_read_csv(self):
#         content = self.s3_store.read(f"folder_for_test_dont_delete/KALUMBURU.csv")
#         self.assertTrue(content is not None)

class LocalStoreTestCase(TestCase):
    def setUp(self):
        with patch('nettle.utils.log_info.LogInfo') as MockClass:
            log = MockClass.return_value
        self.local_store = Local(log=log)

    @classmethod
    def setUpClass(cls):
        os.makedirs(f"nettle_tests/temp_folder_for_test/")

    @classmethod
    def tearDownClass(cls):
        os.rmdir(f"nettle_tests/temp_folder_for_test/")

    def test_fs(self):
        self.assertTrue(self.local_store.fs())
        self.assertTrue(isinstance(self.local_store.fs(), fsspec.implementations.local.LocalFileSystem))

    # def test_list_directory(self):
    #     list directory not implemented yet in Local

    def test_has_existing_file(self):
        self.assertTrue(self.local_store.has_existing_file(f"nettle_tests/fixtures/metadatas.py"))

    def test_has_existing_file_but_file_dont_exist(self):
        self.assertFalse(self.local_store.has_existing_file(f"path_non_existent_d1b034"))

    def test_has_existing_file_full_path(self):
        full_filepath_exists = os.path.join(
            self.local_store.base_folder,
            f"nettle_tests/fixtures/metadatas.py"
        )
        self.assertTrue(self.local_store.has_existing_file_full_path(full_filepath_exists))

    def test_has_existing_file_full_path_but_file_dont_exist(self):
        full_filepath_dont_exist = os.path.join(
            self.local_store.base_folder,
            f"path_non_existent_d1b034"
        )
        self.assertFalse(self.local_store.has_existing_file_full_path(full_filepath_dont_exist))

    def test_write_none_json(self):
        with self.assertRaises(Exception) as cm:
            self.local_store.write(f"nettle_tests/temp_folder_for_test/test_metadata_none.json", None)
        self.assertEqual(
            "[store.write] content file not identified",
            str(cm.exception)
        )
        os.remove(f"nettle_tests/temp_folder_for_test/test_metadata_none.json")

    def test_write_none_csv(self):
        with self.assertRaises(Exception) as cm:
            self.local_store.write(f"nettle_tests/temp_folder_for_test/test_metadata_none.csv", None)
        self.assertEqual(
            "[store.write] content file not identified",
            str(cm.exception)
        )
        os.remove(f"nettle_tests/temp_folder_for_test/test_metadata_none.csv")

    def test_write_without_termination(self):
        # self.local_store.log = Mock()
        # self.local_store.log.warn.assert_called_with('I/O error(2): No such file or directory. Filepath: tests/temp_folder_for_test/test_metadata_without_termination')
        #
        # other option:
        # MagicMock(side_effect=lambda x: print(x))
        content = {"a": 1, "b": 2}
        filepath = self.local_store.write(f"nettle_tests/temp_folder_for_test/test_metadata_without_termination", content)
        self.assertEqual(filepath, f"nettle_tests/temp_folder_for_test/test_metadata_without_termination")
        os.remove(f"nettle_tests/temp_folder_for_test/test_metadata_without_termination")

    def test_write_json(self):
        content = {"a": 1, "b": 2}
        filepath = self.local_store.write(f"nettle_tests/temp_folder_for_test/test_metadata.json", content)
        self.assertEqual(filepath, f"nettle_tests/temp_folder_for_test/test_metadata.json")
        os.remove(f"nettle_tests/temp_folder_for_test/test_metadata.json")

    def test_write_csv(self):
        d = {'col1': [1, 2], 'col2': [3, 4]}
        df = pd.DataFrame(data=d)
        filepath = self.local_store.write(f"nettle_tests/temp_folder_for_test/KALUMBRU.csv", df)
        self.assertEqual(filepath, f"nettle_tests/temp_folder_for_test/KALUMBRU.csv")
        os.remove(f"nettle_tests/temp_folder_for_test/KALUMBRU.csv")

    def test_read_without_termination(self):
        content_non_existent = self.local_store.read(f"metadata")
        self.assertFalse(content_non_existent)

    def test_read_json_non_existent(self):
        content_non_existent = self.local_store.read(f"path_non_existent_d1b034.json")
        self.assertFalse(content_non_existent)

    def test_read_csv_non_existent(self):
        content_non_existent = self.local_store.read(f"path_non_existent_d1b034.csv")
        self.assertFalse(content_non_existent)

    def test_read_json(self):
        content = self.local_store.read(f"nettle_tests/fixtures/metadata.json")
        self.assertTrue(content)

    def test_read_csv(self):
        content = self.local_store.read(f"nettle_tests/fixtures/KALUMBURU.csv")
        self.assertTrue(content is not None)