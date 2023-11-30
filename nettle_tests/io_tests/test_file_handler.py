import os.path
import json
import tempfile
from unittest import TestCase
from nettle.io.file_handler import FileHandler
from nettle_tests.fixtures.metadatas import kalumburu_metadata

class FileHandlerTestCase(TestCase):
    def setUp(self):
        self.relative_path_1 = 'test_etl_path_1_f45e0e20'
        self.relative_path_2 = 'test_etl_path_2_f45e0e20'
        self.file_handler = FileHandler(relative_path=self.relative_path_1)
        self.full_path_2 = os.path.join(self.file_handler.RAW_DATA_ROOT, self.relative_path_2)

    def tearDown(self) -> None:
        os.rmdir(self.file_handler.RAW_DATA_PATH)
        os.rmdir(self.file_handler.PROCESSED_DATA_PATH)

    def test_create_directory_if_necessary(self):
        self.assertFalse(os.path.exists(self.full_path_2))
        self.file_handler.create_directory_if_necessary(self.full_path_2)
        self.assertTrue(os.path.exists(self.full_path_2))
        os.rmdir(self.full_path_2)

    def test_get_data_path_exist(self):
        self.assertTrue(os.path.exists(self.file_handler.RAW_DATA_PATH))
        response_path = self.file_handler.get_data_path(self.file_handler.RAW_DATA_ROOT)
        self.assertEqual(response_path, self.file_handler.RAW_DATA_PATH)

    def test_get_data_path_does_not_exist(self):
        self.assertFalse(os.path.exists(self.full_path_2))
        self.file_handler.relative_path = self.relative_path_2
        response_path = self.file_handler.get_data_path(self.file_handler.RAW_DATA_ROOT)
        self.assertTrue(os.path.exists(self.full_path_2))
        self.assertEqual(response_path, self.full_path_2)
        os.rmdir(self.full_path_2)

    def test_load_dict(self):
        fp = tempfile.NamedTemporaryFile(mode="w+")
        json.dump(kalumburu_metadata, fp)
        fp.flush()
        loaded_dict = self.file_handler.load_dict(fp.name)
        fp.close()
        self.assertEqual(loaded_dict, kalumburu_metadata)

