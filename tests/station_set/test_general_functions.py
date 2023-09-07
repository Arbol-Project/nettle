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

