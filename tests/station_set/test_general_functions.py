import os
import pandas as pd
from unittest import TestCase
from unittest.mock import patch
from nettle.io.store import Local
from tests.fixtures.bom_test import BOMTest
from tests.fixtures.metadatas import kalumburu_metadata

class GeneralFunctionsTestCase(TestCase):
    def setUp(self):
        with patch('nettle.utils.log_info.LogInfo') as MockClass:
            self.log = MockClass.return_value
        self.etl = BOMTest(
            log=self.log,
            store=Local(),
            custom_dict_path=f"tests/fixtures/"
        )

    def test_station_name_formatter(self):
        self.assertEqual(self.etl.station_name_formatter("_auckland aerod&rome/ aws"), 'AUCKLAND_AEROD_ROME_AWS')

    # def test_should_combine__dataframe_with_remote_old_dataframe(self):
    #     d = {'dt': ["2023-06-25", "2023-07-14"], 'col2': [3, 4]}
    #     df = pd.DataFrame(data=d)
    #     self.assertTrue(self.etl.should_combine__dataframe_with_remote_old_dataframe(df, kalumburu_metadata))
    #
    # def test_should_not_combine__dataframe_with_remote_old_dataframe(self):
    #     df = pd.DataFrame(data={'dt': ["2023-08-25", "2023-08-29"], 'col2': [3, 4]})
    #     self.assertFalse(self.etl.should_combine__dataframe_with_remote_old_dataframe(df, kalumburu_metadata))
    #     df = pd.DataFrame(data={'dt': ["2023-08-26", "2023-08-29"], 'col2': [3, 4]})
    #     self.assertFalse(self.etl.should_combine__dataframe_with_remote_old_dataframe(df, kalumburu_metadata))