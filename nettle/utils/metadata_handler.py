import os
import json
from .date_handler import DateHandler


class MetadataHandler:
    METADATA_FILE_NAME = "metadata.json"
    STATION_METADATA_FILE_NAME = "stations.json"
    STATIC_FOLDER = 'static'
    STATION_INFO_FOLDER = 'station_info'
    DATA_DICT_FOLDER = 'data_dictionaries'

    def __init__(self, log, file_handler, dict_path,
                 station_set_name, store):
        self._log = log
        self._file_handler = file_handler
        self.dict_path = dict_path
        self.station_set_name = station_set_name
        self.store = store

    @staticmethod
    def translate_latest_metadata_json_date_format_to_python_datetime(latest_metadata):
        if "date range" in latest_metadata:
            latest_metadata["date range"] = list(
                DateHandler.convert_date_range(latest_metadata["date range"]))
        return latest_metadata

    def get_raw_latest_metadata_new_version(self, path, last_local_output_directory):
        # Check if the method latest_metadata exists in store
        if hasattr(self.store, 'latest_metadata') and callable(getattr(self.store, 'latest_metadata')):
            return self.store.latest_metadata(path, last_local_output_directory=last_local_output_directory)
        else:
            self._log.info(f"Using default metadata {path}")
            if path == self.METADATA_FILE_NAME:
                return {
                    "documentation": {},
                    "api documentation": {}
                }
            elif path == self.STATION_METADATA_FILE_NAME:
                return {
                    "features": []
                }

    def latest_metadata_dict(self, path, last_local_output_directory=None):
        '''
        Returns a dict of the JSON data in the most recently generated metadata file for this set. Checks IPFS first and then falls back
        to the filesystem unless `force_filesystem` is set. Return an empty dict without error if no hash is found for this set. The key
        argument is passed to `self.latest_hash` and can be used to override the class's `self.json_key()`. The root argument is passed
        to `self.last_local_output_directory` and can be used to change the location where metadata is searched for in the case of using
        the filesystem.
        '''
        latest_metadata = self.get_raw_latest_metadata_new_version(
            path, last_local_output_directory)

        latest_metadata = self.translate_latest_metadata_json_date_format_to_python_datetime(
            latest_metadata)

        # insert an api documentation section if necessary
        latest_metadata.setdefault("api documentation", {})

        return latest_metadata

    def get_dict(self, folder: str, dict_name: str = None):
        if dict_name is None:
            dict_name = self.station_set_name

        dict_path = os.path.join(
            self.dict_path, self.STATIC_FOLDER, folder, f"{dict_name}.json")

        return self._file_handler.load_dict(dict_path)

    def get_station_info(self, dict_name: str = None):
        return self.get_dict(self.STATION_INFO_FOLDER, dict_name)

    def get_data_dict(self, dict_name: str = None):
        return self.get_dict(self.DATA_DICT_FOLDER, dict_name)

    def get_latest_metadata(self):
        pass

    def get_latest_station_info_metadata(self):
        pass

    # def get_metadata_dicts(self, data_dict_name: str = None, station_dict_name: str = None):
    #     # If variables are not specified, default to the station_set_name populated from the manager name
    #     if data_dict_name == None:
    #         data_dict_name = self.station_set_name
    #     if station_dict_name == None:
    #         station_dict_name = self.station_set_name
    #     # static data dictionary which gets added
    #     # to self.metadata in write_metadata()
    #     data_dict_path = os.path.join(
    #         self.dict_path, "static", "data_dictionaries", f"{data_dict_name}.json")
    #
    #     # station dictionary containing additional station info
    #     # including geometry if not programmatically available
    #     # Will get converted to geojson in station_metadata_to_geojson()
    #     station_dict_path = os.path.join(
    #         self.dict_path, "static", "station_info", f"{station_dict_name}.json")
    #
    #     # self.DATA_DICT, self.STATION_DICT
    #     return self._file_handler.load_dict(data_dict_path), self._file_handler.load_dict(station_dict_path)

    # metadata

    def latest_metadata(self, root=None, path=METADATA_FILE_NAME):
        return self.latest_metadata_dict(path, self._file_handler.last_local_output_directory(root=root))
