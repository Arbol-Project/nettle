import os
from unittest import TestCase
from unittest.mock import patch
from nettle.io.store import Local
from tests.fixtures.bom_test import BOMTest

class ExtractMethodsTestCase(TestCase):
    def setUp(self):
        with patch('nettle.utils.log_info.LogInfo') as MockClass:
            self.log = MockClass.return_value
        self.etl = BOMTest(
            log=self.log,
            store=Local(),
            custom_dict_path=f"tests/fixtures/"
        )


#
#
# def save_raw_dataframe(
#             self,
#             raw_dataframe: pd.DataFrame,
#             station_id: str,
#             **kwargs
#     ) -> None:
#
# @contextmanager
#     def check_station_extract_loop(
#             self,
#             station_id: str
#     ):
#