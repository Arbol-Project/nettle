import os
import logging
import time
from unittest import TestCase
from unittest.mock import patch
from nettle.io.store import Local
from nettle.metadata.bases import BASE_OUTPUT_METADATA
from nettle.metadata.bases import BASE_OUTPUT_STATION_METADATA
import pandas as pd
from nettle_tests.fixtures.bom_test import BOMTest
from nettle_tests.fixtures.metadatas import kalumburu_metadata
from nettle_tests.fixtures.metadatas import bom_metadata
from nettle.errors.custom_errors import FailedStationException
from nettle.errors.custom_errors import DataframeInvalidException
from nettle.errors.custom_errors import MetadataInvalidException
from nettle.station_set import StationSet

class TransformMethodsTestCase(TestCase):
    def setUp(self):
        self.log = logging.getLogger('').log
        self.etl = BOMTest(
            log=self.log,
            store=Local(),
            custom_dict_path=f"nettle_tests/fixtures/"
        )

    def test_transform(self):
        pass

    def test_get_stations_to_transform(self):
        self.etl.file_handler.RAW_DATA_PATH = f"nettle_tests/fixtures/"
        self.assertEqual(self.etl.get_stations_to_transform(), ['KALUMBURU'])

    @patch('time.time')
    def test_etl_print_runtime(self, mock_time):
        mock_time.return_value = 0
        with self.assertLogs('', level='INFO') as cm:
            with self.etl.etl_print_runtime('STATION_IDENTIFIER'):
                pass

        self.assertEqual(
            cm.output,
            [
                'INFO:root:[bomtest] [transform] station_id=STATION_IDENTIFIER time=\x1b[93m0.00\x1b[0m'
            ]
        )

    def test_single_station_transform(self):
        pass

    def test_check_station_parse_loop(self):
        with self.assertLogs('', level='ERROR') as cm:
            with self.etl.check_station_parse_loop('STATION_IDENTIFIER'):
                raise FailedStationException('Failed message')

        self.assertEqual(
            cm.output,
            [
                'ERROR:root:[bomtest] [transform] transform single station failed for STATION_IDENTIFIER: Failed message'
            ]
        )

    def test_read_raw_station_data(self):
        self.etl.file_handler.RAW_DATA_PATH = f"nettle_tests/fixtures/"
        with self.assertLogs('', level='INFO') as cm:
            df = self.etl.read_raw_station_data('KALUMBURU')

        self.assertFalse(df.empty)
        self.assertEqual(
            cm.output,
            [
                'INFO:root:[bomtest] [read_raw_station_data] read raw station data'
            ]
        )

    def test_get_old_or_default_station_geo_metadata(self):
        with patch('nettle.metadata.metadata_handler.MetadataHandler') as MockClass:
            self.etl.metadata_handler = MockClass.return_value
            self.etl.metadata_handler.get_old_station_geo_metadata.return_value = kalumburu_metadata

        self.assertTrue(self.etl.get_old_or_default_station_geo_metadata("KALUMBURU.geojson"))
        self.assertEqual(self.etl.get_old_or_default_station_geo_metadata("KALUMBURU.geojson")['type'], 'FeatureCollection')
        self.assertEqual(
            self.etl.get_old_or_default_station_geo_metadata("KALUMBURU.geojson")['features'][0]['properties']['station name'],
            'KALUMBURU'
        )

    def test_get_old_or_default_station_geo_metadata_empty(self):
        with patch('nettle.metadata.metadata_handler.MetadataHandler') as MockClass:
            self.etl.metadata_handler = MockClass.return_value
            self.etl.metadata_handler.get_old_station_geo_metadata.return_value = None

        self.assertTrue(self.etl.get_old_or_default_station_geo_metadata("KALUMBURU.geojson"))
        self.assertEqual(self.etl.get_old_or_default_station_geo_metadata("KALUMBURU.geojson")['type'], 'FeatureCollection')
        self.assertEqual(
            self.etl.get_old_or_default_station_geo_metadata("KALUMBURU.geojson")['features'][0]['properties']['station name'],
            ''
        )

    def test_validate_processed_dataframe_failed_columns_not_in_data_dictionary(self):
        d = {'col1': [1, 2], 'col2': [3, 4]}
        df = pd.DataFrame(data=d)

        with self.assertRaises(DataframeInvalidException):
            with self.assertLogs('', level='ERROR') as cm:
                self.etl.validate_processed_dataframe(df)

        self.assertEqual(
            cm.output,
            [
                "ERROR:root:[bomtest] [validate_processed_dataframe] processed dataframe not validated: [dataframevalidator.validate] columns in processed dataframe not in data dictionary: ['col1', 'col2']"
            ]
        )

    def test_validate_processed_dataframe_failed_columns_needs_to_be_string(self):
        d = {'TMIN': [1, 2], 'TMAX': [3, 4]}
        df = pd.DataFrame(data=d)

        with self.assertRaises(DataframeInvalidException):
            with self.assertLogs('', level='ERROR') as cm:
                self.etl.validate_processed_dataframe(df)

        self.assertEqual(
            cm.output,
            [
                "ERROR:root:[bomtest] [validate_processed_dataframe] processed dataframe not validated: [dataframevalidator.validate] all dataframes columns needs to be a string. \nthose columns are not strings: \nTMIN    int64\nTMAX    int64\ndtype: object"
            ]
        )

    def test_validate_processed_dataframe_success(self):
        d = {'TMIN': ['1', '2'], 'TMAX': ['3', '4']}
        df = pd.DataFrame(data=d)
        self.assertIsNone(self.etl.validate_processed_dataframe(df))

    # ToDo: Check this later
    # def test_programmatic_station_metadata_update(self):
    #     pass

    def test_update_date_range_in_station_metadata(self):
        d = {'dt': ['8/8/2023', '8/3/2023', '8/1/2023'], 'col2': [3, 4, 5]}
        df = pd.DataFrame(data=d)
        processed_station_metadata = BASE_OUTPUT_STATION_METADATA
        with patch('nettle.utils.date_range_handler.DateRangeHandler') as MockClass:
            self.etl.date_range_handler = MockClass.return_value
            self.etl.date_range_handler.get_lowest_and_highest_date_range.return_value = ['8/1/2023', '8/8/2023']

        self.etl.update_date_range_in_station_metadata(df, processed_station_metadata)
        self.assertEqual(processed_station_metadata['features'][0]['properties']['date range'], ['8/1/2023', '8/8/2023'])

    def test_update_variables_in_station_metadata(self):
        d = {'dt': ['8/8/2023', '8/3/2023', '8/1/2023'], 'TMIN': [3, 4, 5]}
        df = pd.DataFrame(data=d)
        processed_station_metadata = BASE_OUTPUT_STATION_METADATA

        self.etl.update_variables_in_station_metadata(df, processed_station_metadata)

        variable1 = processed_station_metadata["features"][0]["properties"]["variables"]['0']
        variable2 = processed_station_metadata["features"][0]["properties"]["variables"]['1']
        self.assertEqual(variable1['column name'], 'dt')
        self.assertEqual(variable2['column name'], 'TMIN')

    def test_save_processed_data(self):
        d = {'dt': ['8/8/2023', '8/3/2023', '8/1/2023'], 'col2': [3, 4, 5]}
        df = pd.DataFrame(data=d)
        with patch.object(self.etl, "combine_processed_dataframe_with_remote_old_dataframe") as combine_processed_dataframe_with_remote_old_dataframe:
            with patch.object(self.etl, "save_processed_dataframe") as save_processed_dataframe:
                combine_processed_dataframe_with_remote_old_dataframe.return_value = df
                save_processed_dataframe.return_value = None
                result = self.etl.save_processed_data(df, 'STATION_IDENTIFIER')
                self.assertEqual(result, ['8/1/2023', '8/8/2023'])

    # ToDo: Check this later
    # def test_combine_processed_dataframe_with_remote_old_dataframe(self):
    #     pass

    def test_save_processed_dataframe(self):
        d = {'col1': [1, 2], 'col2': [3, 4]}
        df = pd.DataFrame(data=d)

        with patch('nettle.io.store.Local') as MockClass:
            self.etl.local_store = MockClass.return_value
            self.etl.local_store.write.return_value = 'fixtures/STATION_IDENTIFIER.csv'

        with self.assertLogs('', level='INFO') as cm:
            self.etl.save_processed_dataframe(df, 'STATION_IDENTIFIER')

        self.assertEqual(
            cm.output,
            [
                "INFO:root:[bomtest] [save_processed_dataframe] wrote station file to fixtures/STATION_IDENTIFIER.csv"
            ]
        )

    def test_validate_station_metadata(self):
        # ToDo: Check validate_station_metadata method, seems here should fail
        station_metadata = BASE_OUTPUT_STATION_METADATA
        new_date_range = []
        self.etl.validate_station_metadata(station_metadata, new_date_range)


    def test_save_processed_station_metadata(self):
        with patch('nettle.io.store.Local') as MockClass:
            self.etl.local_store = MockClass.return_value
            self.etl.local_store.write.return_value = 'fixtures/STATION_IDENTIFIER.geojson'

        with self.assertLogs('', level='INFO') as cm:
            self.etl.save_processed_station_metadata(kalumburu_metadata, 'STATION_IDENTIFIER')

        self.assertEqual(
            cm.output,
            [
                "INFO:root:[bomtest] [save_processed_station_metadata] wrote station geojson metadata to fixtures/STATION_IDENTIFIER.geojson"
            ]
        )

    # ToDo: Check this later
    # def test_save_combined_metadata_files(self):
    #     pass

    def test_get_old_or_default_metadata(self):
        with patch('nettle.metadata.metadata_handler.MetadataHandler') as MockClass:
            self.etl.metadata_handler = MockClass.return_value
            self.etl.metadata_handler.get_old_metadata.return_value = bom_metadata

        self.assertTrue(self.etl.get_old_or_default_metadata())
        self.assertEqual(self.etl.get_old_or_default_metadata()['name'], 'BOM Australia Weather Station Data')

    def test_get_old_or_default_metadata_empty(self):
        with patch('nettle.metadata.metadata_handler.MetadataHandler') as MockClass:
            self.etl.metadata_handler = MockClass.return_value
            self.etl.metadata_handler.get_old_metadata.return_value = None

        self.assertTrue(self.etl.get_old_or_default_metadata())
        self.assertEqual(self.etl.get_old_or_default_metadata()['name'], '')

    def test_validate_metadata(self):
        # ToDo: Maybe this should fail?
        metadata = BASE_OUTPUT_METADATA
        self.etl.validate_metadata(metadata)

    def test_validate_metadata_failed_name(self):
        metadata = BASE_OUTPUT_METADATA
        metadata['name'] = 2
        with self.assertRaises(MetadataInvalidException):
            self.etl.validate_metadata(metadata)

    # ToDo: Check this later
    # def test_generate_combined_station_metadata(self):
    #     pass
