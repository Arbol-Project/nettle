from unittest import TestCase
from unittest.mock import patch
from nettle.metadata.metadata_handler import MetadataHandler
from tests.fixtures.data_dicts import bom_data_dict
from tests.fixtures.station_dicts import bom_station_dict
from tests.fixtures.metadatas import bom_metadata
from tests.fixtures.metadatas import kalumburu_metadata

class MetadataHandlerTestCase(TestCase):
    def setUp(self):
        with patch('nettle.io.file_handler.FileHandler') as MockClass:
            self.file_handler = MockClass.return_value
        with patch('nettle.utils.log_info.LogInfo') as MockClass:
            self.log = MockClass.return_value

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

    def test_old_station_geo_metadata_by_store(self):
        # ToDO: Importing store Local here is generating a warning, need to figure out why
        with patch('nettle.io.store.Local') as MockClass:
            store = MockClass.return_value
            store.read.return_value = kalumburu_metadata

        metadata_handler = MetadataHandler(self.file_handler, "somewhere", "BOM", store, store, self.log)
        self.assertEqual(
            metadata_handler.get_old_station_geo_metadata_by_store("KALUMBURU.geojson", store),
            kalumburu_metadata
        )

    def test_get_old_metadata_by_store(self):
        # ToDO: Importing store Local here is generating a warning, need to figure out why
        with patch('nettle.io.store.Local') as MockClass:
            store = MockClass.return_value
            store.read.return_value = bom_metadata

        metadata_handler = MetadataHandler(self.file_handler, "somewhere", "BOM", store, store, self.log)
        self.assertEqual(
            metadata_handler.get_old_metadata_by_store(store),
            bom_metadata
        )

    def test_get_old_metadata(self):
        # ToDO: Importing store Local here is generating a warning, need to figure out why
        with patch('nettle.io.store.Local') as MockClass:
            store = MockClass.return_value
            store.read.return_value = bom_metadata

        metadata_handler = MetadataHandler(self.file_handler, "somewhere", "BOM", store, store, self.log)
        self.assertEqual(
            metadata_handler.get_old_metadata(),
            bom_metadata
        )

    def test_get_old_station_geo_metadata(self):
        # ToDO: Importing store Local here is generating a warning, need to figure out why
        with patch('nettle.io.store.Local') as MockClass:
            store = MockClass.return_value
            store.read.return_value = kalumburu_metadata

        metadata_handler = MetadataHandler(self.file_handler, "somewhere", "BOM", store, store, self.log)
        self.assertEqual(
            metadata_handler.get_old_station_geo_metadata("KALUMBURU.geojson"),
            kalumburu_metadata
        )