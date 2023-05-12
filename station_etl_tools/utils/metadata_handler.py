import os
import json
from .date_handler import DateHandler


class MetadataHandler:
    METADATA_FILE_NAME = "metadata.json"
    STATION_METADATA_FILE_NAME = "stations.json"

    def __init__(self, http_root, log, file_handler, dict_path,
                 station_set_name, custom_metadata_head_path, store):
        # self._ipfs_handler = ipfs_handler
        self._log = log
        self._file_handler = file_handler
        self.http_root = http_root
        self.dict_path = dict_path
        self.station_set_name = station_set_name
        self.custom_metadata_head_path = custom_metadata_head_path
        self.store = store

    def hash_url(self, hash_str, link_name=""):
        '''
        Return URL to a hash + optional link stored on an IPFS HTTP gateway using `self.http_root` as the gateway
        '''
        return f"{self.http_root}/ipfs/{hash_str}/{link_name}"

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

    # def get_raw_latest_metadata(self, custom_metadata_head_path, force_filesystem, key, path,
    #                             metadata_filename, station_filename, latest_hash,
    #                             is_force_http_enabled=False, is_force_filesystem_enabled=False,
    #                             last_local_output_directory=None):
    #     if custom_metadata_head_path is not None:
    #         return self.metadata_by_filesystem("", path=custom_metadata_head_path)
    #     elif force_filesystem:
    #         return self.metadata_by_filesystem(last_local_output_directory, path=path)
    #     elif is_force_http_enabled:
    #         return self.metadata_by_http(latest_hash, path=path)
    #     elif not is_force_filesystem_enabled and latest_hash is not None:
    #         return self.metadata_by_hash(latest_hash, path=path)
    #     elif last_local_output_directory:
    #         return self.metadata_by_filesystem(last_local_output_directory, path=path)
    #     elif path == metadata_filename:
    #         return {
    #             "documentation": {},
    #             "api documentation": {}
    #         }
    #     elif path == station_filename:
    #         return {"features": []}

    def latest_metadata_dict(self, custom_metadata_head_path, force_filesystem, key, path,
                             metadata_filename, station_filename, latest_hash,
                             is_force_http_enabled=False, is_force_filesystem_enabled=False,
                             last_local_output_directory=None):
        '''
        Returns a dict of the JSON data in the most recently generated metadata file for this set. Checks IPFS first and then falls back
        to the filesystem unless `force_filesystem` is set. Return an empty dict without error if no hash is found for this set. The key
        argument is passed to `self.latest_hash` and can be used to override the class's `self.json_key()`. The root argument is passed
        to `self.last_local_output_directory` and can be used to change the location where metadata is searched for in the case of using
        the filesystem.
        '''
        latest_metadata = self.get_raw_latest_metadata_new_version(path, last_local_output_directory)
        # latest_metadata = \
        #     self.get_raw_latest_metadata(custom_metadata_head_path, force_filesystem, key, path,
        #                                  metadata_filename, station_filename, latest_hash,
        #                                  is_force_http_enabled, is_force_filesystem_enabled,
        #                                  last_local_output_directory)

        latest_metadata = self.translate_latest_metadata_json_date_format_to_python_datetime(
            latest_metadata)

        # insert an api documentation section if necessary
        latest_metadata.setdefault("api documentation", {})

        return latest_metadata

    def metadata_by_hash(self, recursive_hash, path):
        '''
        Look up a hash in IPFS and return the metadata.json file associated with it
        '''
        print('IPFS - uses ipfs handler metadata 1')
        return self._ipfs_handler.contents_from_hash(recursive_hash, path, as_json=True)

    def metadata_by_filesystem(self, root, path):
        '''
        Get metadata from local filesystem by passing in a root folder path
        '''
        metadata_path = os.path.join(root, path)
        self._log.info(f"getting metadata from {metadata_path}")
        if os.path.exists(metadata_path):
            with open(metadata_path, "rt") as metadata:
                return json.load(metadata)
        else:
            self._log.error(f"no metadata file found at {metadata_path}")

    def metadata_by_http(self, recursive_hash, path):
        '''
        Connect to the IPFS HTTP API running on `self.http_root` and return the metadata file associated with the desired hash
        '''
        print('IPFS - uses ipfs handler metadata 2')
        return self._ipfs_handler.contents_from_hash(recursive_hash, path, as_json=True)

    def get_metadata_dicts(self):
        # static data dictionary which gets added
        # to self.metadata in write_metadata()
        data_dict_path = os.path.join(
            self.dict_path, "static", "data_dictionaries", f"{self.station_set_name}.json")

        # station dictionary containing additional station info
        # including geometry if not programmatically available
        # Will get converted to geojson in station_metadata_to_geojson()
        station_dict_path = os.path.join(
            self.dict_path, "static", "station_info", f"{self.station_set_name}.json")

        # self.DATA_DICT, self.STATION_DICT
        return self._file_handler.load_dict(data_dict_path), self._file_handler.load_dict(station_dict_path)

    # metadata

    def latest_metadata(self, force_filesystem=False, key=None, root=None, path=METADATA_FILE_NAME):
        return self.latest_metadata_dict(self.custom_metadata_head_path, force_filesystem, key, path,
                                         self.METADATA_FILE_NAME, self.STATION_METADATA_FILE_NAME,
                                         'REMOVE THIS, OLD IPFS STUFF',
                                         False,
                                         False,
                                         self._file_handler.last_local_output_directory(root=root))
