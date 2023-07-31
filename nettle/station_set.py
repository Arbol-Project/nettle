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
import json
from contextlib import contextmanager
from .io.file_handler import FileHandler
from .io.store import Local
from .utils.log_info import LogInfo
from .utils.date_range_handler import DateRangeHandler
from .errors.custom_errors import FailedStationException
from .errors.custom_errors import MetadataInvalidException
from .errors.custom_errors import DataframeInvalidException
from .metadata.bases import BASE_OUTPUT_METADATA
from .metadata.bases import BASE_OUTPUT_STATION_METADATA
from .metadata.metadata_handler import MetadataHandler
from .metadata.validators import metadata_validator
from .metadata.validators import station_metadata_validator
from .dataframe.validators import DataframeValidator as df_validator


class StationSet(ABC):
    '''
    This is an abstract base class for data parsers. It is intended to be inherited and implemented by child classes specific to
    each data source. For example, for data sourced from CHIRPS, there is a CHIRPS general class that implements most of CHIRPS
    parsing. Inheriting from that is a fully implemented CHIRPS05 class which updates, parses, and verifies CHIRPS .05 data.
    '''

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
        self.BASE_OUTPUT_METADATA = BASE_OUTPUT_METADATA
        self.BASE_OUTPUT_STATION_METADATA = BASE_OUTPUT_STATION_METADATA
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
        self.date_range_handler = DateRangeHandler()
        self.file_handler = FileHandler(relative_path=relative_path)
        self.metadata_handler = MetadataHandler(self.file_handler,
                                                StationSet.default_dict_path(),
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

    @staticmethod
    def default_dict_path():
        return os.path.join(os.getcwd())

    @classmethod
    def name(cls):
        '''
        Return the name of instantiated class
        '''
        return f"{cls.__name__}".lower()

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

    def single_station_parse(
            self,
            station_id: str,
            **kwargs
    ) -> None:
        with self.check_station_parse_loop(station_id):
            raw_dataframe = self.read_raw_station_data(station_id, **kwargs)
            raw_station_metadata, processed_dataframe = self.transform_raw_data(raw_dataframe, station_id, **kwargs)
            processed_station_metadata = self.transform_raw_metadata(raw_station_metadata, station_id, **kwargs)
            self.programmatic_station_metadata_update(processed_dataframe, processed_station_metadata, **kwargs)
            self.save_processed_data(processed_dataframe, station_id, **kwargs)
            self.save_processed_station_metadata(processed_station_metadata, station_id, **kwargs)

    def read_raw_station_data(
            self,
            station_id: str,
            **kwargs
    ) -> pd.DataFrame:
        df = self.local_store.read(os.path.join(self.file_handler.RAW_DATA_PATH, f'{station_id}.csv'))
        self.log.info("read raw station data")
        return df

    def save_processed_data(
            self,
            processed_dataframe: pd.DataFrame,
            station_id: str,
            **kwargs
    ) -> None:
        processed_dataframe = self.combine_processed_dataframe_with_remote_old_dataframe(
            processed_dataframe, station_id
        )
        self.save_processed_dataframe(processed_dataframe, station_id, **kwargs)

    def save_processed_dataframe(
            self,
            processed_dataframe: pd.DataFrame,
            station_id: str,
            **kwargs
    ) -> None:
        file_name = f"{self.station_name_formatter(station_id)}.csv"
        filepath = self.local_store.write(
            os.path.join(self.file_handler.PROCESSED_DATA_PATH, file_name),
            processed_dataframe
        )
        self.log.info("wrote station file to {}".format(filepath))

    def save_processed_station_metadata(
            self,
            processed_station_metadata: dict,
            station_id: str,
            **kwargs
    ) -> None:
        self.validate_station_metadata(processed_station_metadata)
        station_filename = f"{self.station_name_formatter(station_id)}.geojson"
        filepath = self.local_store.write(
            os.path.join(self.file_handler.PROCESSED_DATA_PATH, station_filename),
            processed_station_metadata
        )
        self.log.info("wrote station geojson metadata to {}".format(filepath))

    def programmatic_station_metadata_update(
            self,
            processed_dataframe: pd.DataFrame,
            processed_station_metadata: dict,
            **kwargs
    ) -> None:
        self.validate_processed_dataframe(processed_dataframe)
        self.update_date_range_in_station_metadata(processed_dataframe, processed_station_metadata)
        self.update_variables_in_station_metadata(processed_dataframe, processed_station_metadata)

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

    @staticmethod
    def validate_metadata(
            metadata: dict
    ):
        if not metadata_validator.validate(metadata):
            # raise MetadataInvalidException(f"Metadata is invalid: {metadata_validator.errors}")
            raise MetadataInvalidException(
                f"Metadata is invalid: {json.dumps(metadata_validator.errors, indent=2, default=str)}"
            )

    @staticmethod
    def validate_station_metadata(
            station_metadata: dict
    ):
        if not station_metadata_validator.validate(station_metadata):
            # raise MetadataInvalidException(f"Station metadata is invalid: {station_metadata_validator.errors}")
            raise MetadataInvalidException(f"Station metadata is invalid: {json.dumps(station_metadata_validator.errors, indent=2, default=str)}")

    def save_combined_metadata_files(
            self,
            **kwargs
    ) -> None:
        metadata = self.get_metadata()
        self.validate_metadata(metadata)
        filepath = self.local_store.write(
            os.path.join(self.file_handler.PROCESSED_DATA_PATH, MetadataHandler.METADATA_FILE_NAME),
            metadata
        )
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

    def update_date_range_in_station_metadata(
            self,
            processed_dataframe: pd.DataFrame,
            processed_station_metadata: dict,
            **kwargs
    ) -> None:
        new_date_range = self.date_range_handler.get_lowest_and_highest_date_range(
            processed_dataframe, processed_station_metadata
        )
        processed_station_metadata['features'][0]['properties']['date range'] = new_date_range

    def update_variables_in_station_metadata(
            self,
            processed_dataframe: pd.DataFrame,
            processed_station_metadata: dict,
            **kwargs
    ) -> None:
        df_properties = list(processed_dataframe.columns)
        # Get the data dict properties that exist in processed_dataframe
        variables = {key: value for key, value in self.DATA_DICTIONARY.items() if value["column name"] in df_properties}
        processed_station_metadata["features"][0]["properties"]["variables"] = variables

    def validate_processed_dataframe(
            self,
            processed_dataframe: pd.DataFrame
    ) -> None:
        try:
            df_validator.validate(processed_dataframe, self.DATA_DICTIONARY)
        except DataframeInvalidException as die:
            self.log.error(f"Processed dataframe not validated: {str(die)}")
            raise die

    def combine_processed_dataframe_with_remote_old_dataframe(
            self,
            processed_dataframe: pd.DataFrame,
            station_id: str
    ) -> pd.DataFrame:
        # self.log.info("check if needs to combine old data to new data")
        # dataframe_date_begin, dataframe_end_date = self.get_date_range_from_dataframe(processed_dataframe)
        # metadata_date_begin, metadata_date_end = self.get_date_range_from_metadata(station_metadata)
        # begin_date = min(dataframe_date_begin, metadata_date_begin) if metadata_date_begin else dataframe_date_begin
        # end_date = max(dataframe_end_date, metadata_date_end) if metadata_date_end else dataframe_end_date
        #
        # if dataframe_date_begin != begin_date or dataframe_end_date != end_date:

        self.log.info("needs combining old data to new data")
        filename = f'{station_id}.csv'
        old_df = self.store.read(os.path.join(self.file_handler.relative_path, filename))

        # order define the priority of which df should keep the row in case they have the same date
        # In this case processed has the priority
        processed_dataframe['order'] = 1
        if old_df is None:
            self.log.warn(f"Could not find old dataframe {station_id}.csv on {self.store.base_folder}")
            old_df = pd.DataFrame()

        else:
            old_df['order'] = 0
            # processed_dataframe = processed_dataframe.astype({'dt': 'datetime64[ns]'})
            # old_df = old_df.astype({'dt': 'datetime64[ns]'})

        # combine the two
        df = pd.concat([old_df, processed_dataframe])
        joiner_df = df[['dt', 'order']].groupby(
            'dt', as_index=False).max('order')
        final_df = pd.merge(joiner_df, df,
                            how='left', on=['dt', 'order'])
        final_df.sort_values(by='dt', ascending=True,
                             inplace=True, ignore_index=True)
        final_df = final_df.drop(columns=['order'])
        final_df.drop_duplicates(subset='dt', inplace=True, ignore_index=True)

        # else:
        #     final_df = processed_dataframe

        return final_df

# For debug purpose, will be removed in the final version
class TestSet(StationSet):
    def get_metadata(
            self
    ) -> dict:
        """
        Get old in store.read, if does not exist create one based on model
        :return:
        """
        metadata = self.metadata_handler.get_old_metadata()
        if metadata is None:
            metadata = BASE_OUTPUT_METADATA
            metadata['name'] = "BOM Australia Weather Station Data"
            metadata['data source'] = "www.com"
            metadata['compression'] = "None"
            metadata['documentation'] = "Weather data from the Bureau of Meteorology in Australia covering roughly " \
                                        "500 stations"
            metadata['tags'] = ["temperature", "rain", "wind", "australia"]
            metadata['data dictionary'] = self.DATA_DICTIONARY

            if self.store.name() == 'ipfs':
                metadata["previous hash"] = self.store.latest_hash()

        metadata["time generated"] = str(self.today_with_time)
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
        raw_station_metadata = self.metadata_handler.get_old_station_geo_metadata(station_id)
        if raw_station_metadata is None:
            raw_station_metadata = BASE_OUTPUT_STATION_METADATA
            feature = raw_station_metadata['features'][0]
            feature['geometry'] = self.STATION_DICTIONARY[f'{station_id}']['geometry']

            properties = {
                "station name": f"{station_id}",
                "previous hash": None,
                "code": self.STATION_DICTIONARY[f'{station_id}']['code'],
                "country": "",
                "file name": f"{self.station_name_formatter(station_id)}.csv",
                "date range": []
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

        # Do changes in dataframe here
        processed_dataframe = raw_dataframe

        return raw_station_metadata, processed_dataframe

    def transform_raw_metadata(
            self,
            raw_station_metadata: dict,
            station_id: str,
            **kwargs
    ) -> dict:
        # Do change metadata here
        processed_station_metadata = raw_station_metadata

        return processed_station_metadata


