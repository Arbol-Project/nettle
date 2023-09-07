import os
from unittest import TestCase
from unittest.mock import patch
from nettle.io.store import Local
from tests.fixtures.bom_test import BOMTest

class TransformMethodsTestCase(TestCase):
    def setUp(self):
        with patch('nettle.utils.log_info.LogInfo') as MockClass:
            self.log = MockClass.return_value
        self.etl = BOMTest(
            log=self.log,
            store=Local(),
            custom_dict_path=f"tests/fixtures/"
        )


# def transform(self, **kwargs) -> None:
# def get_stations_to_transform(
#
# @contextmanager
#     def etl_print_runtime(
#             self,
#             station_id: str,
#             step: str = 'transform'
#     ):
#
#
# def single_station_transform(self, station_id: str, **kwargs) -> None:
#
#
#
# @contextmanager
#     def check_station_parse_loop(
#             self,
#             station_id: str
#     ):
#
# def read_raw_station_data(
#             self,
#             station_id: str,
#             **kwargs
#     ) -> pd.DataFrame:
#
#
# def get_old_or_default_station_geo_metadata(
#             self,
#             station_id: str
#     ) -> dict:
#
#
# def validate_processed_dataframe(
#             self,
#             processed_dataframe: pd.DataFrame
#     ) -> None:
#
#
# def programmatic_station_metadata_update(
#             self,
#             processed_dataframe: pd.DataFrame,
#             processed_station_metadata: dict,
#             **kwargs
#     ) -> None:
#
#
# def update_date_range_in_station_metadata(
#             self,
#             processed_dataframe: pd.DataFrame,
#             processed_station_metadata: dict,
#             **kwargs
#     ) -> None:
#
#
# def update_variables_in_station_metadata(
#             self,
#             processed_dataframe: pd.DataFrame,
#             processed_station_metadata: dict,
#             **kwargs
#     ) -> None:
#
# def save_processed_data(
#             self,
#             processed_dataframe: pd.DataFrame,
#             station_id: str,
#             **kwargs
#     ) -> None:
#
# def combine_processed_dataframe_with_remote_old_dataframe(
#             self,
#             processed_dataframe: pd.DataFrame,
#             station_id: str
#     ) -> pd.DataFrame:
#
# def save_processed_dataframe(
#             self,
#             processed_dataframe: pd.DataFrame,
#             station_id: str,
#             **kwargs
#     ) -> None:
#
# @staticmethod
#     def validate_station_metadata(
#             station_metadata: dict,
#             new_date_range: list
#     ):
#
# def save_processed_station_metadata(
#             self,
#             processed_station_metadata: dict,
#             station_id: str,
#             **kwargs
#     ) -> None:
#
# def save_combined_metadata_files(
#             self,
#             **kwargs
#     ) -> None:
#
#
# def get_old_or_default_metadata(self) -> dict:
#
# @staticmethod
# def validate_metadata(
#             metadata: dict
#     ):
#
#
# def generate_combined_station_metadata(self) -> dict: