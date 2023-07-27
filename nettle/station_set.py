# StationSet.py
#
# Abstract base classes defining managers for Arbol's climate data sets to be inherited by classes that will implement set-specific
# update, parse, and verification methods

from abc import ABC, abstractmethod

import datetime
import os
import pandas as pd
import time
import re
from contextlib import contextmanager
from .io.file_handler import FileHandler
from .io.store import Local
from .utils.log_info import LogInfo
from .utils.date_handler import DateHandler
from .errors.custom_errors import FailedStationException
from .errors.custom_errors import MetadataInvalidException
from .metadata.bases import BASE_OUTPUT_METADATA
from .metadata.bases import BASE_OUTPUT_STATION_METADATA
from .metadata.metadata_handler import MetadataHandler
from .metadata.validators import metadata_validator
from .metadata.validators import station_metadata_validator
from .dataframe.validators import dataframe_validator
from .dataframe.validators import DataframeValidationErrors


class StationSet(ABC):
    '''
    This is an abstract base class for data parsers. It is intended to be inherited and implemented by child classes specific to
    each data source. For example, for data sourced from CHIRPS, there is a CHIRPS general class that implements most of CHIRPS
    parsing. Inheriting from that is a fully implemented CHIRPS05 class which updates, parses, and verifies CHIRPS .05 data.
    '''
    METADATA_FILE_NAME = "metadata.json"
    STATION_METADATA_FILE_NAME = "stations.json"
    FILE_NAME_FORMAT = "{0}.csv"
    HISTORY_FILE_NAME = "history.json"

    COMPRESSION_NONE = "none"
    COMPRESSION_GZIP = "gzip"

    def __init__(
            self,
            collection,
            dataset,
            log=print,
            custom_relative_data_path=None,
            store=None,
            multithread_transform=None
    ):
        '''
        Set member variables to defaults.
        '''
        # Establish date today just incase etl runs over midnight
        self.today_with_time = datetime.datetime.now()
        self.multithread_transform = multithread_transform
        self.COLLECTION = collection
        self.DATASET = dataset
        self.log = LogInfo(log, self.name())
        if custom_relative_data_path is None:
            relative_path = os.path.join(
                self.COLLECTION,
                self.DATASET
            )
        else:
            relative_path = custom_relative_data_path
        self.store = store
        self.store.log = self.log
        self.local_store = Local(
            log=self.log
        )
        self.date_handler = DateHandler()
        self.file_handler = FileHandler(relative_path=relative_path)
        self.metadata_handler = MetadataHandler(self.file_handler,
                                                self.default_dict_path(),
                                                self.name(),
                                                self.store,
                                                self.local_store,
                                                self.log)

        # If needed a custom name call it again using
        # get_station_dict(dict_name='custom_name')
        self.STATION_DICTIONARY = self.metadata_handler.get_station_info()
        self.DATA_DICTIONARY = self.metadata_handler.get_data_dict()

    def __str__(self):
        return self.name()

    def __eq__(self, other):
        '''
        All instances of this class will compare equal to each other
        '''
        return str(self) == other

    def __hash__(self):
        return hash(str(self))

    def default_dict_path(self):
      return os.path.join(os.getcwd())

    @classmethod
    def name(cls):
        '''
        Return the name of instantiated class
        '''
        return f"{cls.__name__}".lower()

    # maybe set this info for each station set in a json file and read it here?
    # so the class doesnt have to override this class
    # @abstractmethod
    # def climate_measurement_span(self):
    #     '''
    #     Returns the time resolution of the dataset as a string (e.g. "hourly", "daily", "monthly", etc.)
    #     '''
    #     pass

    # def get_historical_dataframe(self, station_id):
    #     """
    #     Try and return the historical data from s3 in the following order:
    #
    #     1) Attempt to find data at dataset/station.csv
    #     2) Attempt to find data at dataset.csv
    #     3) return blank dataframe
    #
    #     """
    #     whole_history_path = f'{self.name().lower()}.csv'
    #     specific_history_path = f'{self.name().lower()}/{station_id}.csv'
    #
    #     dataframe = self.store.read_csv_from_station(
    #         specific_history_path)
    #
    #     if dataframe is None:
    #         dataframe = self.store.read_csv_from_station(
    #             whole_history_path)
    #         print(
    #             f"No historical data found for {station_id} specifically, using whole dataset df")
    #
    #     if dataframe is None:
    #         self.log.info(
    #             f"No historical data found for {station_id}, returning blank df")
    #         dataframe = pd.DataFrame()
    #
    #     return dataframe
    #
    # def update_date_ranges_in_station_dict(self):
    #     """
    #     Get old metadata to the new metadata
    #     """
    #     try:
    #         old_station_metadata = self.metadata_handler.latest_metadata(
    #             path=MetadataHandler.STATION_METADATA_FILE_NAME)
    #
    #         # each feature represents a station
    #         for feature in old_station_metadata["features"]:
    #             # this is the standard field name for station name
    #             # you will use this key to query the api
    #             station_id = feature["properties"]["station name"]
    #             # pull out date range (if exists)
    #             date_range = feature["properties"]["date range"]
    #             if station_id in self.STATION_DICT:
    #                 # update date range
    #                 self.STATION_DICT[station_id]["date range"] = list(
    #                     DateHandler.convert_date_range_str_to_date(date_range))
    #             else:
    #                 raise Exception(
    #                     f'Station {station_id} was in your last run but is no longer'
    #                     f' in your static list of stations, why is this?')
    #     except (FileNotFoundError, IOError, TypeError, KeyError):
    #         print('Likely first run, this is safe')
    #
    # def get_stations_ids(self):
    #     return list(self.STATION_DICTIONARY.keys())
    #
    # @staticmethod
    # def _get_smallest_value_date(data_df):
    #     return data_df["dt"].min().to_pydatetime().date()
    #
    # def _transform_date_range(self, station_id, new_start, start):
    #     # sometimes the api returns data outside the expected range
    #     # if we have run before, set the date range to what we originally wanted
    #     # to retrieve with new_start
    #     if "date range" in self.STATION_DICT[station_id]:
    #         self.STATION_DICT[station_id]["date range"] = [
    #             new_start, self.today_with_time.date()]
    #     # if we haven't run before we can use the minimum of the available data
    #     # as our start date
    #     else:
    #         self.STATION_DICT[station_id]["date range"] = [
    #             start, self.today_with_time.date()]
    #
    # def _transform_active_hash(self, station_id, is_new_start_smaller):
    #     self.STATION_DICT[station_id]["active hash"] = is_new_start_smaller
    #
    # def _transform_station_dict(self, station_id, data):
    #     """
    #     Check if our new_data_df actually contains new data
    #
    #     1) It does contain new data, then update metadata date ranges for the station
    #     2) Update metadata active hash
    #     """
    #     try:
    #         new_start = data['new_start']
    #         new_data_df = data['transform_result']
    #     except KeyError:
    #         self.log.error("Data new_start or transform_result missing")
    #         return
    #
    #     is_new_start_equal_or_smaller = pd.Timestamp(
    #         new_start) <= new_data_df["dt"].max()
    #     if is_new_start_equal_or_smaller:
    #         start = self._get_smallest_value_date(new_data_df)
    #         # if the new data doesn't go back as far as we expected
    #         # raise an error
    #         if start > new_start:
    #             raise Exception(
    #                 f'You are not getting enough data to match your expected '
    #                 f'start date for station {station_id}')
    #         self._transform_date_range(station_id, new_start, start)
    #     self._transform_active_hash(station_id, is_new_start_equal_or_smaller)
    #
    # @staticmethod
    # def _is_start_smaller_than_max_df_date(start, data_df):
    #     return pd.Timestamp(start) <= data_df["dt"].max()
    #
    # @staticmethod
    # def compare_download_date_range(new_start, new_data_df, station_id):
    #     """
    #     Check if our new_data_df actually contains new data
    #
    #     1) It does contain new data, then update metadata date ranges for the station, return 0
    #     2) It doesn't contain new data, return 1
    #
    #     """
    #     return int(pd.Timestamp(new_start) > new_data_df["dt"].max())
    #
    # def _get_first_date_from_station_dict(self, station_id, station_df):
    #     try:
    #         return self.STATION_DICT[station_id]["date range"][0].date()
    #     except AttributeError:
    #         return self.STATION_DICT[station_id]["date range"][0]
    #     except KeyError:
    #         try:
    #             return station_df['dt'].min()
    #         except KeyError:
    #             self.log.error('Problem with station_df[\'dt\']')
    #
    # @staticmethod
    # def _get_proper_dates_from_dataframe(station_df, start):
    #     # convert datetimes to dates
    #     # copy happens to avoid pandas warning
    #     station_df = station_df.copy()
    #     # convert to date, only necessary for daily otherwise we want Timestamp
    #     # probably better to leave everything as a time and remove the 00:00:00 at the end
    #     station_df["dt"] = [date.date() for date in station_df["dt"]]
    #     # then convert to string, why isn't it a string already
    #     # could we just make this the dtype of the column somewhere else
    #     station_df["dt"] = station_df["dt"].astype('object')
    #     # trim to not include data before required start
    #     station_df = station_df.loc[station_df['dt'] >= start]
    #     station_df.reset_index(inplace=True, drop=True)
    #     return station_df
    #
    # @staticmethod
    # def _get_actual_date_range_from_dataframe(station_df):
    #     # now assess date range present vs date range expected
    #     # dataframe dt column
    #     actual_date_range = list(station_df['dt'])
    #     return [str(date) for date in actual_date_range]
    #
    # @staticmethod
    # def _get_expected_date_range_from_dataframe(station_df, freq='D'):
    #     # what dt should look like if one entry per day for every day
    #     expected_date_range = list(pd.date_range(
    #         station_df['dt'].min(), station_df['dt'].max(), freq=freq))
    #     # convert to string, I think the dtype comment solves this nicely
    #     return [str(date.date()) for date in expected_date_range]
    #
    # @staticmethod
    # def use_expected_date_range_and_fill_gaps_with_null(station_df, expected_date_range):
    #     # make expected your dt column
    #     # attach values from actual to it
    #     df_dates = pd.DataFrame(data={'dt': [datetime.datetime.strptime(
    #         date, '%Y-%m-%d').date() for date in expected_date_range]})
    #     return pd.merge(left=df_dates, right=station_df, how='left', on='dt')
    #
    # def _transform_output_station_df_daily(self, station_id, station_df):
    #     # if daily then data should look like
    #     # dt         | value
    #     # ___________|____________
    #     # 2022-01-01 | fish
    #     # 2022-01-02 | chips
    #     # 2022-01-03 | mushy peas
    #     # 2022-01-04 | curry sauce
    #
    #     # cut data down to start where we wanted it to initially
    #     # this is just in case the api has returned extra data
    #     # and can never be because we don't have early enough data
    #     start = self._get_first_date_from_station_dict(station_id, station_df)
    #     station_df = self._get_proper_dates_from_dataframe(station_df, start)
    #     actual_date_range = self._get_actual_date_range_from_dataframe(
    #         station_df)
    #     expected_date_range = self._get_expected_date_range_from_dataframe(
    #         station_df)
    #     return actual_date_range, expected_date_range, station_df
    #
    # def transform_output_station_df_based_on_span(self, station_id, station_df):
    #     """
    #     Check the validity of the output dataframe before proceding to write it out
    #     """
    #     # make sure we have one entry per expected time frame
    #     actual_date_range = ''
    #     expected_date_range = ''
    #
    #     if self.climate_measurement_span() == 'daily':
    #         actual_date_range, expected_date_range, station_df = \
    #             self._transform_output_station_df_daily(station_id, station_df)
    #
    #     # use the expected date range instead of the actual date range and fill gaps with null
    #     if expected_date_range != actual_date_range:
    #         station_df = \
    #             self.use_expected_date_range_and_fill_gaps_with_null(station_df,
    #                                                                  expected_date_range)
    #
    #     return station_df
    #
    # @staticmethod
    # def _get_date_range_from_station_df(station_df):
    #     return [station_df['dt'].min().isoformat(), station_df['dt'].max().isoformat()]
    #
    # def _change_metadata_date_range(self, station_id):
    #     date_range = self.STATION_DICT[station_id]["date range"]
    #     if "date range" not in self.metadata:
    #         self.metadata["date range"] = date_range
    #     else:
    #         self.metadata["date range"][0] = min(
    #             self.metadata["date range"][0], date_range[0])
    #         self.metadata["date range"][1] = max(
    #             self.metadata["date range"][1], date_range[1])
    #
    # def _get_sorted_columns_in_dict_from_station_df(self, station_df):
    #     variables = {}
    #     for key, value in self.DATA_DICT.items():
    #         if value["column name"] in list(station_df.columns):
    #             variables[key] = value
    #     # variables.sort()
    #     return variables
    #
    # def write_info_in_station_dict(self, station_id, data):
    #     try:
    #         station_df = data['df']
    #     except KeyError as e:
    #         message = 'data[\'df\'] not setted in on_parse_transform'
    #         self.log.error(message)
    #         raise e
    #     # use the date range for each station to change the overall metadata.json date range
    #     date_range = self._get_date_range_from_station_df(station_df)
    #     # get the columns in each file, raise an exception if you can't find them all in the data dict
    #     variables = self._get_sorted_columns_in_dict_from_station_df(
    #         station_df)
    #
    #     self.STATION_DICT[station_id][
    #         "file name"] = f"{self.station_name_formatter(station_id)}.csv"
    #     self.STATION_DICT[station_id]["date range"] = date_range
    #     # When we assign a dict directly to a variable and later on we change that
    #     # variable. The initial dict will be changed too
    #     # Example:
    #     # At the end of run self.DATA_DICT['1']["unit of measurement"] should be an array
    #     # of units used: ['deg_C', 'deg_F'] or ['deg_F'] or ['deg_C']
    #     # self.STATION_DICT[station_id]["variables"] = self.DATA_DICT
    #     # self.STATION_DICT[station_id]["variables"]['1']["unit of measurement"] = 'deg_C'
    #     # So now self.DATA_DICT['1']["unit of measurement"] will be 'deg_C', even if before
    #     # we used 'deg_F' in another station, when in reality it should have been an array of
    #     # units.
    #     # To prevent that we use a deepcopy so we the changes in STATION_DICT are not
    #     # reflected in DATA_DICT
    #     self.STATION_DICT[station_id]["variables"] = copy.deepcopy(variables)
    #
    # def load_verify(self, station_id, data, **kwargs):
    #     station_df = data['df']
    #     variables = self.STATION_DICT[station_id]["variables"]
    #
    #     if len(variables) != len(station_df.columns):
    #         raise Exception(
    #             f"There are {len(station_df.columns)} column(s) in your dataframe being published but "
    #             f"only {len(variables)} column(s) could be found in the data dictionary, please investigate")
    #
    #
    # def write_station_file(self, station_id, station_df, **kwargs):
    #     '''
    #     Write out a single file for a station, making sure to save relevant info such as date range
    #     to self.STATION_DICT. Should be called when no further changes are required to station_df
    #     '''
    #     file_name = f"{self.station_name_formatter(station_id)}.csv"
    #     local_store = Local(dataset_manager=self)
    #     filepath = local_store.write(file_name, station_df)
    #     self.log.info("wrote station file to {}".format(filepath))
    #
    # def write_metadata(self, data, **kwargs):
    #     '''
    #     Write a JSON file containing the metadata dict to the output path as `self.METADATA_FILE_NAME`. If the 'date range' field
    #     is a datetime object, it will be written as iso format in the JSON file. 'final through' will also be converted
    #     '''
    #     self.metadata["data dictionary"] = self.DATA_DICT
    #
    #     metadata_formatted = dict(self.metadata)
    #     if "date range" in self.metadata and hasattr(self.metadata.get("date range")[0], "isoformat"):
    #         metadata_formatted["date range"] = [d.isoformat()
    #                                             for d in self.metadata["date range"]]
    #     if hasattr(self.metadata.get("final through"), "isoformat"):
    #         metadata_formatted["final through"] = self.metadata["final through"].isoformat(
    #         )
    #
    #     local_store = Local(dataset_manager=self)
    #     filepath = local_store.write(
    #         MetadataHandler.METADATA_FILE_NAME, metadata_formatted)
    #     self.log.info("wrote metadata to {}".format(filepath))
    #
    # @staticmethod
    # def _parse_old_stations_from_metadata(old_station_metadata):
    #     return [old_station_metadata['features'][i]["properties"]
    #             ["station name"] for i in range(len(old_station_metadata["features"]))]
    #
    # def _get_old_metadata(self):
    #     """
    #     TODO make md_path toggle actually usable
    #     """
    #     try:
    #         old_station_metadata = self.metadata_handler.latest_metadata(
    #             path=self.metadata_handler.STATION_METADATA_FILE_NAME)
    #         old_stations = self._parse_old_stations_from_metadata(
    #             old_station_metadata)
    #         if self.store.name() == 'ipfs':
    #             old_hash = self.store.latest_hash()
    #         else:
    #             old_hash = None
    #     except (FileNotFoundError, IOError, TypeError, KeyError):
    #         # first run
    #         old_station_metadata = {
    #             "type": "FeatureCollection", "features": []}
    #         old_stations = []
    #         old_hash = None  # is this correct?
    #
    #     return old_station_metadata, old_stations, old_hash
    #
    # def station_metadata_to_geojson(self, data, **kwargs):
    #     '''
    #     Take the station metadata (presumed self.STATION_DICT) and convert it to valid JSON and geojson
    #     '''
    #     old_station_metadata, old_stations, old_hash = self._get_old_metadata()
    #     geojson = {"type": "FeatureCollection", "features": []}
    #
    #     self.geo_json_handler.append_features(geojson, old_stations, old_hash,
    #                                           self.STATION_DICT.items())
    #     self.geo_json_handler.write_geojson_to_file_with_geometry_info(
    #         geojson, self, **kwargs)
    #     # write each stations metadata to its own file to save on retrieval time
    #     for station_geojson in geojson['features']:
    #         station_filename = f"{self.station_name_formatter(station_geojson['properties']['station name'])}.geojson"
    #         station_geojson = {
    #             "type": "FeatureCollection",
    #             "features": [station_geojson]}
    #         self.geo_json_handler.write_geojson_to_file_custom_path(
    #             geojson=station_geojson, data_manager=self, file_name=station_filename, **kwargs)
    #     self.geo_json_handler.remove_geometry_from_geojson(geojson)
    #     self.geo_json_handler.append_stations_not_in_new_update_to_metadata(
    #         old_station_metadata, old_stations, geojson)
    #     self.geo_json_handler.write_geojson_to_file_without_geometry_info(
    #         geojson, self, **kwargs)
    #
    # @staticmethod
    # def _check_prepared_initial_data(prepared_initial_data):
    #     if (type(prepared_initial_data) is not dict or
    #             'stations_ids' not in prepared_initial_data):
    #         raise KeyError('You must return a dict with station_ids in '
    #                        'update_prepare_initial_data')
    #
    # def before_update_prepare_initial_data(self, **kwargs):
    #     # Should we move this to __init in StationSet?
    #     self.DATA_DICT, self.STATION_DICT = self.metadata_handler.get_metadata_dicts()
    #
    #     if self.DATA_DICT is None:
    #         self.log.info('No data dictionary found')
    #
    #     if self.STATION_DICT is None:
    #         self.log.info('No station dictionary found')
    #     else:
    #         # pull the old station metadata and use it to update date ranges on a station by station basis
    #         self.update_date_ranges_in_station_dict()
    #         return {
    #             'stations_ids': self.get_stations_ids()
    #         }
    #
    # def on_update_prepare_initial_data(self, initial_data, **kwargs):
    #     pass
    #
    # def after_update_prepare_initial_data(self, initial_data, **kwargs):
    #     self._check_prepared_initial_data(initial_data)
    #     return initial_data
    #
    # def update_prepare_initial_data(self, **kwargs):
    #     initial_data = self.before_update_prepare_initial_data()
    #     self.on_update_prepare_initial_data(initial_data)
    #     self.after_update_prepare_initial_data(initial_data)
    #     return initial_data
    #
    # def before_update_extract(self, station_id, data, **kwargs):
    #     pass
    #
    # def on_update_extract(self, station_id, data, **kwargs):
    #     pass
    #
    # def after_update_extract(self, station_id, data, **kwargs):
    #     pass
    #
    # def update_extract(self, station_id, data, **kwargs):
    #     self.before_update_extract(station_id, data)
    #     self.on_update_extract(station_id, data, **kwargs)
    #     # self.update_extract(station_id, data)
    #
    # def before_update_transform(self, station_id, data, **kwargs):
    #     pass
    #
    # def on_update_transform(self, station_id, data, **kwargs):
    #     pass
    #
    # def after_update_transform(self, station_id, data, **kwargs):
    #     self._transform_station_dict(station_id, data)
    #
    # def update_transform(self, station_id, data, **kwargs):
    #     # self.before_update_transform(station_id, extract_result, initial_data, **kwargs)
    #     self.on_update_transform(station_id, data, **kwargs)
    #     self.after_update_transform(station_id, data, **kwargs)
    #
    # def on_update_load(self, station_id, data, **kwargs):
    #     pass
    #
    # def update_load(self, station_id, data, **kwargs):
    #     # self.before_update_load(station_id, data, **kwargs)
    #     self.on_update_load(station_id, data, **kwargs)
    #     # self.after_update_load(station_id, data, **kwargs)
    #
    # def on_update_station_verify(self, station_id, data, **kwargs):
    #     pass
    #
    # def update_station_verify(self, station_id, data, **kwargs):
    #     # data['station_verify_result'] = self.on_update_station_verify(station_id, initial_data, extract_result, transform_result, **kwargs)
    #     self.on_update_station_verify(station_id, data, **kwargs)
    #     # data['station_verify_result'] = self.on_update_station_verify(station_id, initial_data, extract_result, transform_result, **kwargs)
    #
    # def on_update_verify(self, data, **kwargs):
    #     pass
    #
    # def update_verify(self, data, **kwargs):
    #     # self.before_update_verify(data, **kwargs)
    #     result = self.on_update_verify(data, **kwargs)
    #     # self.after_update_verify(data, **kwargs)
    #     return result
    #
    # def update_local_input(self, **kwargs):
    #     data = self.update_prepare_initial_data()
    #
    #     for station_id in data['stations_ids']:
    #         try:
    #             t1 = time.time()
    #
    #             self.update_extract(station_id, data)
    #             self.update_transform(station_id, data)
    #             self.update_load(station_id, data)
    #             self.update_station_verify(station_id, data)
    #
    #             t2 = time.time()
    #             self.log.info(
    #                 f'Station_id={station_id} Time=\033[93m{(t2 - t1):.2f}\033[0m')
    #         except FailedStationException as se:
    #             self.log.error(
    #                 f"Update Local Station failed for {station_id}: {str(se)}")
    #
    #     # All stations are up to date, no need to update so return False
    #     return self.update_verify(data)
    #
    # def before_parse_initial_data(self, **kwargs):
    #     return {
    #         'stations_ids': self.get_stations_ids()
    #     }
    #
    # def on_parse_initial_data(self, data, **kwargs):
    #     pass
    #
    # def parse_initial_data(self, **kwargs):
    #     data = self.before_parse_initial_data(**kwargs)
    #     self.on_parse_initial_data(data, **kwargs)
    #     return data
    #
    # def on_parse_extract(self, station_id, data, **kwargs):
    #     pass
    #
    # def parse_extract(self, station_id, data, **kwargs):
    #     # data = self.before_parse_extract(station_id, data, **kwargs)
    #     self.on_parse_extract(station_id, data, **kwargs)
    #     # data = self.after_parse_extract(station_id, data, **kwargs)
    #
    # def parse_transform(self, station_id, data, **kwargs):
    #     self.before_parse_transform(station_id, data, **kwargs)
    #     self.on_parse_transform(station_id, data, **kwargs)
    #     self.after_parse_transform(station_id, data, **kwargs)
    #
    # def before_parse_transform(self, station_id, data, **kwargs):
    #     pass
    #
    # def on_parse_transform(self, station_id, data, **kwargs):
    #     pass
    #
    # def after_parse_transform(self, station_id, data, **kwargs):
    #     try:
    #         data_df = data['df']
    #     except KeyError:
    #         self.log.error('data[\'df\'] not set in on_parse_transform')
    #         return
    #
    #     # make sure station_df fits time span
    #     station_df = self.transform_output_station_df_based_on_span(station_id,
    #                                                                 station_df=data_df)
    #     data['df'] = station_df
    #
    # def on_parse_load(self, station_id, data, **kwargs):
    #     pass
    #
    # def parse_load(self, station_id, data, **kwargs):
    #     # self.before_parse_load(station_id, transform_result, **kwargs)
    #     self.on_parse_load(station_id, data, **kwargs)
    #     self.after_parse_load(station_id, data, **kwargs)
    #
    # def after_parse_load(self, station_id, data, **kwargs):
    #     self.write_info_in_station_dict(station_id, data)
    #     self._change_metadata_date_range(station_id)
    #     self.load_verify(station_id, data)
    #
    #     self.file_handler.create_output_path()
    #     # write file and extract important metadata
    #     self.write_station_file(station_id, station_df=data['df'], **kwargs)
    #
    # def on_parse_verify(self, data, **kwargs):
    #     pass
    #
    # def parse_verify(self, data, **kwargs):
    #     # data = self.before_parse_verify(data, **kwargs)
    #     data = self.on_parse_verify(data, **kwargs)
    #     # data = self.after_parse_verify(data, **kwargs)
    #     return data
    #
    # def parse_bkp(self, **kwargs):
    #     """
    #     Outputs separate csv for each station
    #     formats and writes all metadata
    #     """
    #     data = self.parse_initial_data(**kwargs)
    #     for station_id in data['stations_ids']:
    #         t1 = time.time()
    #         try:
    #             self.parse_extract(station_id, data, **kwargs)
    #             self.parse_transform(station_id, data, **kwargs)
    #             self.parse_load(station_id, data, **kwargs)
    #             t2 = time.time()
    #             self.log.info(
    #                 f'Station_id={station_id} Time=\033[93m{(t2 - t1):.2f}\033[0m')
    #         except FailedStationException as se:
    #             self.log.error(
    #                 f"Parse Station failed for {station_id}: {str(se)}")
    #
    #     self.write_metadata(data, **kwargs)  # write metadata.json
    #     # write stations.json and stations.geojson
    #     self.station_metadata_to_geojson(data, **kwargs)
    #     return self.parse_verify(data, **kwargs)
    #
    # def verify(self, **kwargs):
    #     '''
    #     Check parsed data against the input files by comparing the stored hashes of data already present before the
    #     parse operation and checking newly parsed data entry-by-entry against the input files
    #     '''
    #     pass

    @staticmethod
    def station_name_formatter(station_name: str):
        """
        Take in a string and return it following our convention for station names
        This is:

        1) Upper case
        2) All non-alphanumeric characters are _
        3) No more than 1 _ in a row
        4) First and last character shouldn't be underscores

        Parameters
        ----------
        station_name : str
            A given station name, for example "auckland aerodrome aws"

        Returns
        -------
        formatted_station_name : str
            The same station name but in our conventional format, e.g. "AUCKLAND_AERODROME_AWS"
        """
        # Convert all non alphanumeric to _
        formatted_station_name = re.sub(r'\W+', '_', station_name.upper())
        try:
            # trim underscores from end of string
            while formatted_station_name[-1] == '_':
                formatted_station_name = formatted_station_name[:-1]
            # trim underscores from start of string
            while formatted_station_name[0] == '_':
                formatted_station_name = formatted_station_name[1:]
        except IndexError as e:
            raise Exception(
                f"Sorry, {station_name} cannot be processed properly and is worth investigating")
        return formatted_station_name

    ## NEW STUFF

    def read_raw_station_data(
            self,
            station_id: str,
            **kwargs
    ) -> pd.DataFrame:
        self.local_store.base_folder = self.file_handler.RAW_DATA_PATH
        df = self.local_store.read(f'{station_id}.csv')
        self.log.info("read raw station data")
        return df

    @abstractmethod
    def transform_raw_data(
            self,
            raw_dataframe: pd.DataFrame,
            station_id: str,
            **kwargs
    ) -> tuple[dict, pd.DataFrame]:
        pass

    @abstractmethod
    def transform_raw_metadata(
            self,
            raw_station_metadata: dict,
            station_id: str,
            **kwargs
    ) -> dict:
        pass

    def validate_processed_dataframe(
            self,
            processed_dataframe: pd.DataFrame
    ) -> None:
        try:
            dataframe_validator.validate(processed_dataframe, lazy=True)
        except DataframeValidationErrors as err:
            self.log.error(
                f"Dataframe Validation Errors: "
                f"\n{err.failure_cases}"         # dataframe of schema errors
                f"\n{self.log.error(err.data)}" # invalid dataframe
            )


    def save_processed_data(
            self,
            processed_dataframe: pd.DataFrame,
            station_id: str,
            **kwargs
    ) -> None:
        self.validate_processed_dataframe(processed_dataframe)
        # ToDo: We should check the date range, if differs we get the dataframe from s3 and
        # update locally if still doesnt match we dispatch an error
        self.save_processed_dataframe(processed_dataframe, station_id, **kwargs)

    def save_processed_dataframe(
            self,
            processed_dataframe: pd.DataFrame,
            station_id: str,
            **kwargs
    ) -> None:
        file_name = f"{self.station_name_formatter(station_id)}.csv"
        self.local_store.base_folder = self.file_handler.PROCESSED_DATA_PATH
        filepath = self.local_store.write(file_name, processed_dataframe)
        self.log.info("wrote station file to {}".format(filepath))

    def save_processed_station_metadata(
            self,
            processed_station_metadata: dict,
            station_id: str,
            **kwargs
    ) -> None:
        self.validate_station_metadata(processed_station_metadata)
        station_filename = f"{self.station_name_formatter(station_id)}.geojson"
        self.local_store.base_folder = self.file_handler.PROCESSED_DATA_PATH
        filepath = self.local_store.write(station_filename, processed_station_metadata)
        self.log.info("wrote station geojson metadata to {}".format(filepath))

    @staticmethod
    def validate_metadata(
            metadata: dict
    ):
        if not metadata_validator.validate(metadata):
            raise MetadataInvalidException(f"Metadata is invalid: {metadata_validator.errors}")

    @staticmethod
    def validate_station_metadata(
            station_metadata: dict
    ):
        if not station_metadata_validator.validate(station_metadata):
            raise MetadataInvalidException(f"Station metadata is invalid: {station_metadata_validator.errors}")

    def save_combined_metadata_files(
            self,
            **kwargs
    ) -> None:
        metadata = self.get_metadata()
        self.validate_metadata(metadata)
        self.local_store.base_folder = self.file_handler.PROCESSED_DATA_PATH
        filepath = self.local_store.write(MetadataHandler.METADATA_FILE_NAME, metadata)
        self.log.info("wrote metadata to {}".format(filepath))

    def get_stations_to_transform(
            self
    ) -> list:
        stations = os.listdir(self.file_handler.RAW_DATA_PATH)
        # -4 cuts off .csv
        return [station[:-4] for station in stations if '.csv' in station]

    @contextmanager
    def etl_print_runtime(
            self,
            station_id: str
    ):
        start_time = time.time()
        yield
        finish_time = time.time()
        self.log.info(
            f'Station_id={station_id} Time=\033[93m{(finish_time - start_time):.2f}\033[0m')

    @contextmanager
    def check_station_parse_loop(
            self,
            station_id: str
    ):
        try:
            yield
        except FailedStationException as fse:
            self.log.error(
                f"Transform single station failed for {station_id}: {str(fse)}")

    @abstractmethod
    def get_metadata(
            self
    ):
        """
        Probably get the old metadata or BASE_OUTPUT_METADATA
        :return:
        """
        pass

    @abstractmethod
    def get_base_station_geo_metadata(
            self,
            station_id: str
    ):
        """
        Probably get the old station geo metadata or BASE_OUTPUT_STATION_METADATA
        :return:
        """
        pass

    @staticmethod
    def get_date_range_from_dataframe(
            dataframe: pd.DataFrame
    ) -> tuple[datetime.date, datetime.date]:
        return DateHandler.convert_date_range_str_to_date(min(dataframe['dt']), max(dataframe['dt']))

    @staticmethod
    def get_date_range_from_metadata(
            station_metadata: dict
    ) -> tuple[datetime.date, datetime.date] | tuple[None, None]:
        if station_metadata['features'][0]['properties']['date range']:
            date_begin_str = station_metadata['features'][0]['properties']['date range'][0]
            date_end_str = station_metadata['features'][0]['properties']['date range'][1]

            if date_begin_str and date_end_str:
                return DateHandler.convert_date_range_str_to_date(date_begin_str, date_end_str)
        return None, None

    def single_station_parse(
            self,
            station_id: str,
            **kwargs
    ) -> None:
        with self.check_station_parse_loop(station_id):
            raw_dataframe = self.read_raw_station_data(station_id, **kwargs)
            # 1 - Get raw_data in the final format for processed_data
            # 2 - Extract any information required for individual station_metadata and put in date range
            raw_station_metadata, processed_dataframe = self.transform_raw_data(raw_dataframe, station_id, **kwargs)
            processed_station_metadata = self.transform_raw_metadata(
                raw_station_metadata, station_id, **kwargs)
            self.save_processed_data(processed_dataframe, station_id, **kwargs)
            self.save_processed_station_metadata(processed_station_metadata, station_id, **kwargs)

    def transform(
            self,
            **kwargs
    ) -> None:
        if self.multithread_transform:
            # multithreaded logic for single_station_parse
            pass
        else:
            stations = self.get_stations_to_transform()
            for station_id in stations:
                with self.etl_print_runtime(station_id):
                    self.single_station_parse(station_id, **kwargs)
        self.save_combined_metadata_files(**kwargs)

    def get_old_metadata(
            self
    ) -> dict:
        # Get old metadata using current store
        old_metadata = self.metadata_handler.get_old_metadata()
        if old_metadata is None:
            # Get old metadata using local_store (Look into processed_data folder)
            self.local_store.base_folder = self.file_handler.PROCESSED_DATA_PATH
            old_metadata = self.metadata_handler.get_old_metadata(self.local_store)

        if old_metadata is None:
            self.log.warn(f"Could not find an old metadata")
        return old_metadata

    def get_old_station_geo_metadata(
            self,
            station_id: str
    ) -> dict:
        # Get old station metadata using current store
        old_station_metadata = self.metadata_handler.get_old_station_geo_metadata(station_id)

        if old_station_metadata is None:
            # Get old station metadata using local_store (Look into processed_data folder)
            self.local_store.base_folder = self.file_handler.PROCESSED_DATA_PATH
            old_station_metadata = self.metadata_handler.get_old_station_geo_metadata(station_id, self.local_store)

        if old_station_metadata is None:
            self.log.warn(f"Could not find an old station metadata")
        return old_station_metadata

    def new_station_metadata_date_range(
            self,
            dataframe: pd.DataFrame,
            station_metadata: dict
    ) -> tuple[str, str]:
        dataframe_date_begin, dataframe_end_date = self.get_date_range_from_dataframe(dataframe)
        metadata_date_begin, metadata_date_end = self.get_date_range_from_metadata(station_metadata)
        begin_date = min(dataframe_date_begin, metadata_date_begin) if metadata_date_begin else dataframe_date_begin
        end_date = max(dataframe_end_date, metadata_date_end) if metadata_date_end else dataframe_end_date
        return DateHandler.convert_date_range_date_to_str(begin_date, end_date)

# For debug purpose, will be removed in the final version
class TestSet(StationSet):
    def get_metadata(
            self
    ) -> dict:
        """
        Get old in store.read, if does not exist create one based on model
        :return:
        """
        metadata = self.get_old_metadata()
        if metadata is None:
            metadata = BASE_OUTPUT_METADATA
            metadata['name'] = "BOM Australia Weather Station Data"
            metadata['data source'] = "www.com"
            metadata['compression'] = "None"
            metadata['documentation'] = "Weather data from the Bureau of Meteorology in Australia covering roughly " \
                                        "500 stations"
            metadata['tags'] = ["temperature", "rain", "wind", "australia"]
            metadata['data dictionary'] = self.DATA_DICTIONARY
            metadata["time generated"] = str(self.today_with_time)
            if self.store.name() == 'ipfs':
                metadata["previous hash"] = self.store.latest_hash()

        return metadata

    def get_base_station_geo_metadata(
            self,
            station_id: str
    ) -> dict:
        """
        Get old in store.read, if does not exist create one based on model
        :param station_id:
        :return:
        """
        raw_station_metadata = self.get_old_station_geo_metadata(station_id)
        if raw_station_metadata is None:
            raw_station_metadata = BASE_OUTPUT_STATION_METADATA
            feature = raw_station_metadata['features'][0]
            feature['geometry'] = self.STATION_DICTIONARY[f'{station_id}']['geometry']

            # ToDo: For each self.STATION_DICTIONARY[f'{station_id}']['variables'] Get it from DATA_DICTIONARY
            properties = {
                "station name": f"{station_id}",
                "previous hash": None,
                "code": self.STATION_DICTIONARY[f'{station_id}']['code'],
                "country": "",
                "file name": f"{self.station_name_formatter(station_id)}.csv",
                "date range": [],
                "variables": self.DATA_DICTIONARY # ToDo: Change this
            }
            feature['properties'] = properties
            raw_station_metadata['features'][0] = feature

        return raw_station_metadata

    def transform_raw_data(
            self,
            raw_dataframe: pd.DataFrame,
            station_id: str,
            **kwargs
    ) -> tuple[dict, pd.DataFrame]:
        raw_station_metadata = self.get_base_station_geo_metadata(station_id)
        new_date_range = self.new_station_metadata_date_range(raw_dataframe, raw_station_metadata)
        raw_station_metadata['features'][0]['properties']['date range'] = new_date_range

        # Do process the dataframe in this step
        processed_dataframe = raw_dataframe
        return raw_station_metadata, processed_dataframe

    def transform_raw_metadata(
            self,
            raw_station_metadata: dict,
            station_id: str,
            **kwargs
    ) -> dict:
        # Do process the station metadata in this step
        processed_station_metadata = raw_station_metadata
        return processed_station_metadata


