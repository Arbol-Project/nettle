import os
import logging
import time
from unittest import TestCase
from unittest.mock import patch
from nettle.io.store import Local
from tests.fixtures.bom_test import BOMTest
from nettle.errors.custom_errors import FailedStationException

class TransformMethodsTestCase(TestCase):
    def setUp(self):
        self.log = logging.getLogger('').log
        self.etl = BOMTest(
            log=self.log,
            store=Local(),
            custom_dict_path=f"tests/fixtures/"
        )

    def test_transform(self):
        pass

    def test_get_stations_to_transform(self):
        self.etl.file_handler.RAW_DATA_PATH = f"tests/fixtures/"
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
        pass

    def test_get_old_or_default_station_geo_metadata(self):
        pass

    def test_validate_processed_dataframe(self):
        pass

    def test_programmatic_station_metadata_update(self):
        pass

    def test_update_date_range_in_station_metadata(self):
        pass

    def test_update_variables_in_station_metadata(self):
        pass

    def test_save_processed_data(self):
        pass

    def test_combine_processed_dataframe_with_remote_old_dataframe(self):
        pass

    def test_save_processed_dataframe(self):
        pass

    def test_validate_station_metadata(self):
        pass

    def test_save_processed_station_metadata(self):
        pass

    def test_save_combined_metadata_files(self):
        pass

    def test_get_old_or_default_metadata(self):
        pass

    def test_validate_metadata(self):
        pass

    def test_generate_combined_station_metadata(self):
        pass
