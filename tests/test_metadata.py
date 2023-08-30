from unittest import TestCase
from unittest.mock import patch
from nettle.metadata.metadata_handler import MetadataHandler
from nettle.io.file_handler import FileHandler
from nettle.metadata.metadata_handler import MetadataHandler
from nettle.metadata.metadata_handler import MetadataHandler

class MetadataHandlerTestCase(TestCase):
    def setUp(self):
        with patch('nettle.io.file_handler.FileHandler') as MockClass:
            file_handler = MockClass.return_value
        with patch('nettle.io.store.Local') as MockClass:
            store = MockClass.return_value
        with patch('nettle.utils.log_info.LogInfo') as MockClass:
            log = MockClass.return_value
            # self.metadata_handler.file_handler = 'test1'
            # self.metadata_handler.default_dict_path = 'test2'
        self.met = MetadataHandler(file_handler, "somewhere", "BOM", store, store, log)

    def test_get_dict(self):
        print(self.met.get_dict("dict_folder", "BOM"))
        #ToDo: Stop using Magic

    def test_get_station_info(self):
        raise NotImplementedError("error")

    def test_get_data_dict(self):
        raise NotImplementedError("error")

    def test_get_metadata(self):
        raise NotImplementedError("error")

    def test_old_station_geo_metadata_by_store(self):
        raise NotImplementedError("error")

    def test_get_old_metadata_by_store(self):
        raise NotImplementedError("error")

    def test_get_old_metadata(self):
        raise NotImplementedError("error")

    def test_get_old_station_geo_metadata(self):
        raise NotImplementedError("error")