# StationSet.py
#
# Abstract base classes defining managers for Arbol's climate data sets to be inherited by classes that will implement set-specific
# update, parse, and verification methods

from abc import ABC, abstractmethod

import datetime
import json
import os
import sys
import pandas as pd
from .conf import settings
import time
from .generate import *
from collections.abc import Iterator
from .utils.log_info import LogInfo
from .utils.ipfs_handler import IpfsHandler
from .utils.file_handler import FileHandler
from .utils.metadata_handler import MetadataHandler
from .utils.date_handler import DateHandler
from .utils.s3_handler import S3Handler
from .utils.geo_json_handler import GeoJsonHandler


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

    ### CONSIDER REMOVING ###
    # paths relative to the script directory
    OUTPUT_ROOT = settings.OUTPUT_ROOT
    HASHES_OUTPUT_ROOT = settings.HASHES_OUTPUT_ROOT

    # Is this used? Maybe move these to ipfs
    # HASHES_OUTPUT_ROOT
    # HASH_HISTORY_PATH
    # HISTORY_FILE_NAME
    HASH_HISTORY_PATH = os.path.join(HASHES_OUTPUT_ROOT, HISTORY_FILE_NAME)
    ### END CONSIDER REMOVING ###

    def __init__(self, log=print, custom_output_path=None, custom_metadata_head_path=None,
                 custom_latest_hash=None, publish_to_ipns=False, rebuild=False, force_http=False,
                 custom_input_path=None, http_root=settings.GATEWAY_URL
                 ):
        '''
        Set member variables to defaults. Set the libeccodes lookup path.
        '''
        self.new_files = []
        self.log = LogInfo(log, self.name())
        self.ipfs = None
        self.custom_latest_hash = custom_latest_hash
        self.publish_to_ipns = publish_to_ipns
        self.metadata = None
        self.rebuild = rebuild
        self.custom_output_path = custom_output_path
        # Establish date today just incase etl runs over midnight
        self.today_with_time = datetime.datetime.now()

        self.date_handler = DateHandler()
        self.ipfs_handler = IpfsHandler(force_http, self.log,
                                        http_root, custom_output_path, self.name(),
                                        self.climate_measurement_span())
        self.file_handler = FileHandler(custom_input_path, custom_output_path,
                                        self.climate_measurement_span(),
                                        self.today_with_time,
                                        relative_path=self.name())

        self.metadata_handler = MetadataHandler(self.ipfs_handler, http_root,
                                                self.log, self.file_handler,
                                                self._correct_dict_path(),
                                                self.name(), custom_metadata_head_path)
        self.s3_handler = S3Handler(self.log)
        self.geo_json_handler = GeoJsonHandler(self.file_handler, self.log)
        self.STATION_DICT = {}
        self.DATA_DICT = {}

    def __str__(self):
        return self.name()

    def __eq__(self, other):
        '''
        All instances of this class will compare equal to each other
        '''
        return str(self) == other

    def __hash__(self):
        return hash(str(self))

    def _correct_dict_path(self):
        # Don't hardcode like this
        # Check if name is in valid_source_keys in sources.py
        if self.name() == 'bom2' or self.name() == 'cwv2':
            return os.path.dirname(__file__)
        else:
            return os.getcwd()

    def rebuild_requested(self):
        '''
        Returns True if the rebuild flag is enabled. This should be checked at points where previously existing data is ordinarily used,
        and if necessary, an alternate method should be substituted.
        '''
        return self.rebuild

    @classmethod
    def name(cls):
        '''
        Return the name of instantiated class
        '''
        return f"{cls.__name__}".lower()

    # maybe set this info for each station set in a json file and read it here?
    # so the class doesnt have to override this class
    @abstractmethod
    def climate_measurement_span(self):
        '''
        Returns the time resolution of the dataset as a string (e.g. "hourly", "daily", "monthly", etc.)
        '''
        pass

    def get_historical_dataframe(self, station_id):
        """
        Try and return the historical data from s3 in the following order:

        1) Attempt to find data at dataset/station.csv
        2) Attempt to find data at dataset.csv
        3) return blank dataframe

        """
        whole_history_path = f'{self.name().lower()}.csv'
        specific_history_path = f'{self.name().lower()}/{station_id}.csv'
        dataframe = self.s3_handler.read_csv_from_station(specific_history_path)

        if dataframe is None:
            dataframe = self.s3_handler.read_csv_from_station(whole_history_path)
            print(
                f"No historical data found for {station_id} specifically, using whole dataset df")

        if dataframe is None:
            self.log.info(
                f"No historical data found for {station_id}, returning blank df")
            dataframe = pd.DataFrame()

        return dataframe

    def update_date_ranges_in_station_dict(self):
        """
        Get old metadata to the new metadata
        """
        try:
            old_station_metadata = self.metadata_handler.latest_metadata(
                path=MetadataHandler.STATION_METADATA_FILE_NAME)

            # each feature represents a station
            for feature in old_station_metadata["features"]:
                # this is the standard field name for station name
                # you will use this key to query the api
                station_id = feature["properties"]["station name"]
                # pull out date range (if exists)
                date_range = feature["properties"]["date range"]
                if station_id in self.STATION_DICT:
                    # update date range
                    self.STATION_DICT[station_id]["date range"] = list(
                        DateHandler.convert_date_range(date_range))
                else:
                    raise Exception(
                        f'Station {station_id} was in your last run but is no longer'
                        f' in your static list of stations, why is this?')
        except TypeError:
            print('Likely first run, this is safe')

    def get_station_ids(self):
        return list(self.STATION_DICT.keys())

    @staticmethod
    def _get_smallest_value_date(data_df):
        return data_df["dt"].min().to_pydatetime().date()

    def _transform_date_range(self, station_id, new_start, start):
        # sometimes the api returns data outside the expected range
        # if we have run before, set the date range to what we originally wanted
        # to retrieve with new_start
        if "date range" in self.STATION_DICT[station_id]:
            self.STATION_DICT[station_id]["date range"] = [
                new_start, self.today_with_time.date()]
        # if we haven't run before we can use the minimum of the available data
        # as our start date
        else:
            self.STATION_DICT[station_id]["date range"] = [
                start, self.today_with_time.date()]

    def _transform_active_hash(self, station_id, is_new_start_smaller):
        self.STATION_DICT[station_id]["active hash"] = is_new_start_smaller

    def _transform_station_dict(self, station_id, data):
        """
        Check if our new_data_df actually contains new data

        1) It does contain new data, then update metadata date ranges for the station
        2) Update metadata active hash
        """
        try:
            new_start = data['new_start']
            new_data_df = data['transform_result']
        except KeyError:
            self.log.error("Data new_start or transform_result missing")
            return

        is_new_start_equal_or_smaller = pd.Timestamp(
            new_start) <= new_data_df["dt"].max()
        if is_new_start_equal_or_smaller:
            start = self._get_smallest_value_date(new_data_df)
            # if the new data doesn't go back as far as we expected
            # raise an error
            if start > new_start:
                raise Exception(
                    f'You are not getting enough data to match your expected '
                    f'start date for station {station_id}')
            self._transform_date_range(station_id, new_start, start)
        self._transform_active_hash(station_id, is_new_start_equal_or_smaller)

    @staticmethod
    def _is_start_smaller_than_max_df_date(start, data_df):
        return pd.Timestamp(start) <= data_df["dt"].max()

    @staticmethod
    def compare_download_date_range(new_start, new_data_df, station_id):
        """
        Check if our new_data_df actually contains new data

        1) It does contain new data, then update metadata date ranges for the station, return 0
        2) It doesn't contain new data, return 1

        """
        return int(pd.Timestamp(new_start) > new_data_df["dt"].max())

    def _get_first_date_from_station_dict(self, station_id, station_df):
        try:
            return self.STATION_DICT[station_id]["date range"][0].date()
        except AttributeError:
            return self.STATION_DICT[station_id]["date range"][0]
        except KeyError:
            try:
                return station_df['dt'].min()
            except KeyError:
                self.log.error('Problem with station_df[\'dt\']')

    @staticmethod
    def _get_proper_dates_from_dataframe(station_df, start):
        # convert datetimes to dates
        # copy happens to avoid pandas warning
        station_df = station_df.copy()
        # convert to date, only necessary for daily otherwise we want Timestamp
        # probably better to leave everything as a time and remove the 00:00:00 at the end
        station_df["dt"] = [date.date() for date in station_df["dt"]]
        # then convert to string, why isn't it a string already
        # could we just make this the dtype of the column somewhere else
        station_df["dt"] = station_df["dt"].astype('object')
        # trim to not include data before required start
        station_df = station_df.loc[station_df['dt'] >= start]
        station_df.reset_index(inplace=True, drop=True)
        return station_df

    @staticmethod
    def _get_actual_date_range_from_dataframe(station_df):
        # now assess date range present vs date range expected
        # dataframe dt column
        actual_date_range = list(station_df['dt'])
        return [str(date) for date in actual_date_range]

    @staticmethod
    def _get_expected_date_range_from_dataframe(station_df, freq='D'):
        # what dt should look like if one entry per day for every day
        expected_date_range = list(pd.date_range(
            station_df['dt'].min(), station_df['dt'].max(), freq=freq))
        # convert to string, I think the dtype comment solves this nicely
        return [str(date.date()) for date in expected_date_range]

    @staticmethod
    def use_expected_date_range_and_fill_gaps_with_null(station_df, expected_date_range):
        # make expected your dt column
        # attach values from actual to it
        df_dates = pd.DataFrame(data={'dt': [datetime.datetime.strptime(
            date, '%Y-%m-%d').date() for date in expected_date_range]})
        return pd.merge(left=df_dates, right=station_df, how='left', on='dt')

    def _transform_output_station_df_daily(self, station_id, station_df):
        # if daily then data should look like
        # dt         | value
        # ___________|_________
        # 2022-01-01 | fish
        # 2022-01-02 | chips
        # 2022-01-03 | peas
        # 2022-01-04 | sausage

        # cut data down to start where we wanted it to initially
        # this is just in case the api has returned extra data
        # and can never be because we don't have early enough data
        start = self._get_first_date_from_station_dict(station_id, station_df)
        station_df = self._get_proper_dates_from_dataframe(station_df, start)
        actual_date_range = self._get_actual_date_range_from_dataframe(
            station_df)
        expected_date_range = self._get_expected_date_range_from_dataframe(
            station_df)
        return actual_date_range, expected_date_range, station_df

    def transform_output_station_df_based_on_span(self, station_id, station_df):
        """
        Check the validity of the output dataframe before proceding to write it out
        """
        # make sure we have one entry per expected time frame
        actual_date_range = ''
        expected_date_range = ''

        if self.climate_measurement_span() == 'daily':
            actual_date_range, expected_date_range, station_df = \
                self._transform_output_station_df_daily(station_id, station_df)

        # use the expected date range instead of the actual date range and fill gaps with null
        if expected_date_range != actual_date_range:
            station_df = \
                self.use_expected_date_range_and_fill_gaps_with_null(station_df,
                                                                     expected_date_range)

        return station_df

    @staticmethod
    def _get_date_range_from_station_df(station_df):
        return [station_df['dt'].min().isoformat(), station_df['dt'].max().isoformat()]

    def _change_metadata_date_range(self, date_range):
        if "date range" not in self.metadata:
            self.metadata["date range"] = date_range
        else:
            self.metadata["date range"][0] = min(
                self.metadata["date range"][0], date_range[0])
            self.metadata["date range"][1] = max(
                self.metadata["date range"][1], date_range[1])

    def _get_sorted_columns_in_dict_from_station_df(self, station_df):
        variables = []
        for key, value in self.DATA_DICT.items():
            if value["column name"] in list(station_df.columns):
                variables.append(key)
        variables.sort()
        return variables

    def write_info_in_station_dict(self, station_id, station_df):
        # use the date range for each station to change the overall metadata.json date range
        date_range = self._get_date_range_from_station_df(station_df)
        # get the columns in each file, raise an exception if you can't find them all in the data dict
        variables = self._get_sorted_columns_in_dict_from_station_df(
            station_df)

        self._change_metadata_date_range(date_range)

        # this doesnt seem to do anything
        self.STATION_DICT[station_id]["file name"] = f"{station_id}.csv"
        self.STATION_DICT[station_id]["date range"] = date_range
        self.STATION_DICT[station_id]["variables"] = variables

    def load_verify(self, station_id, station_df, **kwargs):
        variables = self.STATION_DICT[station_id]["variables"]

        if len(variables) != len(station_df.columns):
            raise Exception(
                f"There are {len(station_df.columns)} column(s) in your dataframe being published but only {len(variables)} column(s) could be found in the data dictionary, please investigate")

    def write_station_file(self, station_id, station_df, **kwargs):
        '''
        Write out a single file for a station, making sure to save relevant info such as date range
        to self.STATION_DICT. Should be called when no further changes are required to station_df
        '''
        file_name = f"{station_id}.csv"
        outpath = os.path.join(self.file_handler.output_path(), file_name)
        station_df.to_csv(outpath, index=False)
        self.log.info("wrote station file to {}".format(outpath))

    def write_metadata(self):
        '''
        Write a JSON file containing the metadata dict to the output path as `self.METADATA_FILE_NAME`. If the 'date range' field
        is a datetime object, it will be written as iso format in the JSON file. 'final through' will also be converted
        '''
        self.metadata["data dictionary"] = self.DATA_DICT

        metadata_formatted = dict(self.metadata)
        if "date range" in self.metadata and hasattr(self.metadata.get("date range")[0], "isoformat"):
            metadata_formatted["date range"] = [d.isoformat()
                                                for d in self.metadata["date range"]]
        if hasattr(self.metadata.get("final through"), "isoformat"):
            metadata_formatted["final through"] = self.metadata["final through"].isoformat(
            )
        path = os.path.join(self.file_handler.output_path(),
                            MetadataHandler.METADATA_FILE_NAME)
        with open(path, 'w') as fp:
            json.dump(metadata_formatted, fp, sort_keys=False, indent=4)

        self.log.info("wrote metadata to {}".format(path))

    @staticmethod
    def _parse_old_stations_from_metadata(old_station_metadata):
        return [old_station_metadata['features'][i]["properties"]
                ["station name"] for i in range(len(old_station_metadata["features"]))]

    def _get_old_metadata(self):
        try:
            old_station_metadata = self.metadata_handler.latest_metadata(
                path=MetadataHandler.STATION_METADATA_FILE_NAME)
            old_stations = self._parse_old_stations_from_metadata(
                old_station_metadata)
            old_hash = self.ipfs_handler.latest_hash()
        except TypeError:
            # first run
            old_station_metadata = {
                "type": "FeatureCollection", "features": []}
            old_stations = []
            old_hash = None  # is this correct?

        return old_station_metadata, old_stations, old_hash

    def station_metadata_to_geojson(self):
        '''
        Take the station metadata self.STATION_DICT and convert it to valid geojson
        '''
        old_station_metadata, old_stations, old_hash = self._get_old_metadata()

        geojson = {"type": "FeatureCollection", "features": []}

        self.geo_json_handler.append_features(geojson, old_stations, old_hash,
                                              self.STATION_DICT.items())
        self.geo_json_handler.write_geojson_to_file_with_geometry_info(geojson)
        self.geo_json_handler.remove_geometry_from_geojson(geojson)
        self.geo_json_handler.append_stations_not_in_new_update_to_metadata(
            old_station_metadata, old_stations, geojson)
        self.geo_json_handler.write_geojson_to_file_without_geometry_info(
            geojson)

    @staticmethod
    def _check_prepared_initial_data(prepared_initial_data):
        if (type(prepared_initial_data) is not dict or
                'stations_ids' not in prepared_initial_data):
            raise KeyError('You must return a dict with station_ids in '
                           'update_prepare_initial_data')

    def before_update_prepare_initial_data(self, **kwargs):
        # Should we move this to __init in StationSet?
        self.DATA_DICT, self.STATION_DICT = self.metadata_handler.get_metadata_dicts()

        # pull the old station metadata and use it to update date ranges on a station by station basis
        self.update_date_ranges_in_station_dict()
        return {
            'stations_ids': self.get_station_ids()
        }

    @abstractmethod
    def on_update_prepare_initial_data(self, initial_data, **kwargs):
        pass

    def after_update_prepare_initial_data(self, initial_data, **kwargs):
        self._check_prepared_initial_data(initial_data)
        return initial_data

    def update_prepare_initial_data(self, **kwargs):
        initial_data = self.before_update_prepare_initial_data()
        self.on_update_prepare_initial_data(initial_data)
        self.after_update_prepare_initial_data(initial_data)
        return initial_data

    def before_update_extract(self, station_id, data, **kwargs):
        pass

    @abstractmethod
    def on_update_extract(self, station_id, data, **kwargs):
        pass

    def after_update_extract(self, station_id, data, **kwargs):
        pass

    def update_extract(self, station_id, data, **kwargs):
        self.before_update_extract(station_id, data)
        self.on_update_extract(station_id, data, **kwargs)
        # self.update_extract(station_id, data)

    def before_update_transform(self, station_id, data, **kwargs):
        pass

    @abstractmethod
    def on_update_transform(self, station_id, data, **kwargs):
        pass

    def after_update_transform(self, station_id, data, **kwargs):
        self._transform_station_dict(station_id, data)

    def update_transform(self, station_id, data, **kwargs):
        # self.before_update_transform(station_id, extract_result, initial_data, **kwargs)
        self.on_update_transform(station_id, data, **kwargs)
        self.after_update_transform(station_id, data, **kwargs)

    @abstractmethod
    def on_update_load(self, station_id, data, **kwargs):
        pass

    def update_load(self, station_id, data, **kwargs):
        # self.before_update_load(station_id, data, **kwargs)
        self.on_update_load(station_id, data, **kwargs)
        # self.after_update_load(station_id, data, **kwargs)

    @abstractmethod
    def on_update_station_verify(self, station_id, data, **kwargs):
        pass

    def update_station_verify(self, station_id, data, **kwargs):
        # data['station_verify_result'] = self.on_update_station_verify(station_id, initial_data, extract_result, transform_result, **kwargs)
        self.on_update_station_verify(station_id, data, **kwargs)
        # data['station_verify_result'] = self.on_update_station_verify(station_id, initial_data, extract_result, transform_result, **kwargs)

    @abstractmethod
    def on_update_verify(self, data, **kwargs):
        pass

    def update_verify(self, data, **kwargs):
        # self.before_update_verify(data, **kwargs)
        result = self.on_update_verify(data, **kwargs)
        # self.after_update_verify(data, **kwargs)
        return result

    # At some point this info should be set
    # data['extract_result']
    # data['transform_result']
    # data['df']
    # # maybe
    # data['new_start']
    # data['station_verify_result']
    def update_local_input(self, **kwargs):
        data = self.update_prepare_initial_data()

        for station_id in data['stations_ids']:
            t1 = time.time()

            self.update_extract(station_id, data)
            self.update_transform(station_id, data)
            self.update_load(station_id, data)
            self.update_station_verify(station_id, data)

            t2 = time.time()
            self.log.info(
                f'Station_id={station_id} Time=\033[93m{(t2 - t1):.2f}\033[0m')

        # All stations are up to date, no need to update so return False
        return self.update_verify(data)

    @staticmethod
    def before_parse_initial_data(**kwargs):
        return {}
        # data_dict, station_dict = self.metadata_handler.get_metadata_dicts()
        # if not self.DATA_DICT:
        #     self.DATA_DICT = data_dict
        # if not self.STATION_DICT:
        #     self.STATION_DICT = station_dict

    @abstractmethod
    def on_parse_initial_data(self, data, **kwargs):
        pass

    def parse_initial_data(self, **kwargs):
        data = self.before_parse_initial_data(**kwargs)
        self.on_parse_initial_data(data, **kwargs)
        return data

    @abstractmethod
    def on_parse_extract(self, station_id, data, **kwargs):
        pass

    def parse_extract(self, station_id, data, **kwargs):
        # data = self.before_parse_extract(station_id, data, **kwargs)
        self.on_parse_extract(station_id, data, **kwargs)
        # data = self.after_parse_extract(station_id, data, **kwargs)

    def parse_transform(self, station_id, data, **kwargs):
        self.before_parse_transform(station_id, data, **kwargs)
        self.on_parse_transform(station_id, data, **kwargs)
        self.after_parse_transform(station_id, data, **kwargs)

    def before_parse_transform(self, station_id, data, **kwargs):
        pass

    @abstractmethod
    def on_parse_transform(self, station_id, data, **kwargs):
        # make sure data is in correct format
        pass

    def after_parse_transform(self, station_id, data, **kwargs):
        try:
            data_df = data['df']
        except KeyError:
            self.log.error('data[\'df\'] not setted in on_parse_transform')
            return

        # make sure station_df is up to snuff
        station_df = self.transform_output_station_df_based_on_span(station_id,
                                                                    station_df=data_df)
        data['df'] = station_df

    @abstractmethod
    def on_parse_load(self, station_id, data, **kwargs):
        pass

    def parse_load(self, station_id, data, **kwargs):
        # self.before_parse_load(station_id, transform_result, **kwargs)
        self.on_parse_load(station_id, data, **kwargs)
        self.after_parse_load(station_id, data, **kwargs)

    # this will try to access self.metadata, if it doenst exist will crash
    # maybe move its instanciation from station to stationset
    def after_parse_load(self, station_id, data, **kwargs):
        try:
            station_df = data['df']
        except KeyError:
            self.log.error('data[\'df\'] not setted in on_parse_transform')
            return

        # maybe try to move this to after transform? need to move instantiation
        # of self.metadata first
        self.write_info_in_station_dict(station_id, station_df)
        self.load_verify(station_id, station_df)

        self.file_handler.create_output_path()
        # write file and extract important metadata
        self.write_station_file(station_id, station_df=data['df'])

    @abstractmethod
    def on_parse_verify(self, **kwargs):
        pass

    def parse_verify(self, **kwargs):
        # data = self.before_parse_verify(**kwargs)
        data = self.on_parse_verify(**kwargs)
        # data = self.after_parse_verify(**kwargs)
        return data

    def parse(self, **kwargs):
        """
        Outputs separate csv for each station
        formats and writes all metadata
        """
        data = self.parse_initial_data(**kwargs)
        for station_id in self.STATION_DICT.keys():
            t1 = time.time()
            self.parse_extract(station_id, data, **kwargs)
            self.parse_transform(station_id, data, **kwargs)
            self.parse_load(station_id, data, **kwargs)
            t2 = time.time()
            self.log.info(
                f'Station_id={station_id} Time=\033[93m{(t2 - t1):.2f}\033[0m')

        self.write_metadata()  # write metadata.json
        self.station_metadata_to_geojson()  # write stations.json
        return self.parse_verify(**kwargs)

    # is this necessary? I'm tagging this to remove
    # should add to our station-climate-etl-ipfs roadmap
    # testing/verify
    @abstractmethod
    def verify(self, **kwargs):
        '''
        Check parsed data against the input files by comparing the stored hashes of data already present before the 
        parse operation and checking newly parsed data entry-by-entry against the input files
        '''
        pass

    def add_to_ipfs(self, suppress_adding_to_heads=False, recursive=False, path=None,
                    key=None):
        '''
        Add contents of directory at `path` to IPFS. If `path` is `None`, it defaults
        to `self.output_path()`. If `suppress_adding_to_heads` is `False`, store
        resulting hash in heads file, overwriting any hash already stored for this set.
        Recursive flag only has to be set if there are directories within the passed
        directory that also need to be added.
        '''
        if path is None:
            path = self.file_handler.output_path()

        return self.ipfs_handler.add_directory_to_ipfs(path, key, self.publish_to_ipns,
                                                       suppress_adding_to_heads,
                                                       recursive)

    @classmethod
    def generate(cls, **kwargs):
        generate_result(cls, **kwargs)

    @classmethod
    def get_subclasses(cls) -> Iterator:
        """Create a generator with all the subclasses and sub-subclasses of a parent class"""
        for subclass in cls.__subclasses__():
            yield from subclass.get_subclasses()
            yield subclass
