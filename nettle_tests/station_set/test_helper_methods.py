import os
from unittest import TestCase
from unittest.mock import patch
from nettle.io.store import Local
from nettle_tests.fixtures.bom_test import BOMTest

class HelperMethodsTestCase(TestCase):
    def setUp(self):
        with patch('nettle.utils.log_info.LogInfo') as MockClass:
            self.log = MockClass.return_value
        self.etl = BOMTest(
            log=self.log,
            store=Local(),
            custom_dict_path=f"nettle_tests/fixtures/"
        )

    def test__str__(self):
        self.assertEqual(str(self.etl), 'bomtest')

    def test__eq__(self):
        self.etl2 = BOMTest(
            log=self.log,
            store=Local(),
            custom_dict_path=f"nettle_tests/fixtures/"
        )
        self.assertTrue(self.etl == self.etl2)

    def test__hash__(self):
        etl_hash = hash(self.etl)
        self.assertTrue(isinstance(etl_hash, int))

    def test_default_dict_path(self):
        self.assertEqual(self.etl.default_dict_path(), f"nettle_tests/fixtures/")
        self.etl.custom_dict_path = None
        self.assertEqual(self.etl.default_dict_path(), os.path.join(os.getcwd(), 'non_gridded_etl_managers'))

    def test_name(self):
        self.assertEqual(self.etl.name(), 'bomtest')