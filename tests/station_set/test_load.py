import os
from unittest import TestCase
from unittest.mock import patch
from nettle.io.store import Local
from tests.fixtures.bom_test import BOMTest

class LoadTestCase(TestCase):
    def setUp(self):
        with patch('nettle.utils.log_info.LogInfo') as MockClass:
            self.log = MockClass.return_value
        self.etl = BOMTest(
            log=self.log,
            store=Local(),
            custom_dict_path=f"tests/fixtures/"
        )


#
# def cp_folder_to_remote_store(
#             self,
#             custom_local_full_path: str = None,
#             custom_s3_relative_path: str = None
#     ) -> None:
#