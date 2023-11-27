import os
from unittest import TestCase
from unittest.mock import patch
from nettle.io.store import Local
from nettle_tests.fixtures.bom_test import BOMTest

class LoadTestCase(TestCase):
    def setUp(self):
        with patch('nettle.utils.log_info.LogInfo') as MockClass:
            self.log = MockClass.return_value
        self.etl = BOMTest(
            log=self.log,
            store=Local(),
            custom_dict_path=f"tests/fixtures/"
        )

    # This is a system test to check a copy from local to s3
    # def test_cp_folder_to_remote_store(self):
    #     pass
