from unittest import TestCase
from unittest.mock import patch
from nettle.metadata.metadata_handler import MetadataHandler
from .fixtures.data_dicts import bom_data_dict
from .fixtures.station_dicts import bom_station_dict
from .fixtures.metadatas import bom_metadata

class MetadataHandlerTestCase(TestCase):
    def setUp(self):
        with patch('nettle.io.file_handler.FileHandler') as MockClass:
            self.file_handler = MockClass.return_value
        with patch('nettle.utils.log_info.LogInfo') as MockClass:
            self.log = MockClass.return_value
            # self.metadata_handler.file_handler = 'test1'
            # self.metadata_handler.default_dict_path = 'test2'

    def test_get_dict(self):
        # ToDO: Importing store Local here is generating a warning, need to figure out why
        with patch('nettle.io.store.Local') as MockClass:
            store = MockClass.return_value
            store.read.return_value = bom_data_dict

        metadata_handler = MetadataHandler(self.file_handler, "somewhere", "BOM", store, store, self.log)
        self.assertEqual(
            metadata_handler.get_dict("dict_folder", "BOM"),
            bom_data_dict
        )

    def test_get_station_info(self):
        # ToDO: Importing store Local here is generating a warning, need to figure out why
        with patch('nettle.io.store.Local') as MockClass:
            store = MockClass.return_value
            store.read.return_value = bom_station_dict

        metadata_handler = MetadataHandler(self.file_handler, "somewhere", "BOM", store, store, self.log)
        self.assertEqual(
            metadata_handler.get_station_info("BOM"),
            bom_station_dict
        )

    def test_get_data_dict(self):
        # ToDO: Importing store Local here is generating a warning, need to figure out why
        with patch('nettle.io.store.Local') as MockClass:
            store = MockClass.return_value
            store.read.return_value = bom_data_dict

        metadata_handler = MetadataHandler(self.file_handler, "somewhere", "BOM", store, store, self.log)
        self.assertEqual(
            metadata_handler.get_data_dict("BOM"),
            bom_data_dict
        )

    def test_get_metadata(self):
        # ToDO: Importing store Local here is generating a warning, need to figure out why
        with patch('nettle.io.store.Local') as MockClass:
            store = MockClass.return_value
            store.read.return_value = bom_metadata

        metadata_handler = MetadataHandler(self.file_handler, "somewhere", "BOM", store, store, self.log)
        self.assertEqual(
            metadata_handler.get_metadata("metadata.json", store),
            bom_metadata
        )

    # def test_old_station_geo_metadata_by_store(self):
    #     raise NotImplementedError("error")
    #
    # def test_get_old_metadata_by_store(self):
    #     raise NotImplementedError("error")
    #
    # def test_get_old_metadata(self):
    #     raise NotImplementedError("error")
    #
    # def test_get_old_station_geo_metadata(self):
    #     raise NotImplementedError("error")