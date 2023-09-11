import os
import logging
from unittest import TestCase
import pandas as pd
from unittest.mock import patch
from nettle.io.store import Local
from nettle.utils.log_info import LogInfo
from nettle.errors.custom_errors import FailedStationException
from tests.fixtures.bom_test import BOMTest

class ExtractMethodsTestCase(TestCase):
    def setUp(self):
        self.log = logging.getLogger('').log
        self.etl = BOMTest(
            log=self.log,
            store=Local(),
            custom_dict_path=f"tests/fixtures/"
        )

    @classmethod
    def setUpClass(cls):
        os.makedirs(f"tests/temp_folder_for_test/")

    @classmethod
    def tearDownClass(cls):
        os.rmdir(f"tests/temp_folder_for_test/")

    def test_save_raw_dataframe(self):
        d = {'col1': [1, 2], 'col2': [3, 4]}
        raw_dataframe = pd.DataFrame(data=d)
        station_id = 'STATION_IDENTIFIER'

        file_name = f"{self.etl.station_name_formatter(station_id)}.csv"
        self.etl.file_handler.RAW_DATA_PATH = 'tests/temp_folder_for_test/'
        filepath = os.path.join(self.etl.file_handler.RAW_DATA_PATH, file_name)

        with self.assertLogs('', level='INFO') as cm:
            self.etl.save_raw_dataframe(raw_dataframe, station_id)

        self.assertEqual(
            cm.output,
            [
                'INFO:root:[bomtest] [extract] wrote station file to {}'.format(filepath)
            ]
        )
        self.assertTrue(os.path.exists(filepath))
        os.remove(filepath)


    def test_check_station_extract_loop(self):
        with self.assertLogs('', level='ERROR') as cm:
            with self.etl.check_station_extract_loop('STATION_IDENTIFIER'):
                raise FailedStationException('Reason why it failed')

        self.assertEqual(
            cm.output,
            [
                'ERROR:root:[bomtest] [extract] update Local Station failed for STATION_IDENTIFIER: Reason why it failed'
            ]
        )
