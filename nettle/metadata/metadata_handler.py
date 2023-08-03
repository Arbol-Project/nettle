import os


class MetadataHandler:
    METADATA_FILE_NAME = "metadata.json"
    STATION_METADATA_FILE_NAME = "stations.json"
    STATIC_FOLDER = 'static'
    STATION_INFO_FOLDER = 'station_info'
    DATA_DICT_FOLDER = 'data_dictionaries'
    MODULE_FOLDER = 'non_gridded_etl_managers'

    def __init__(self, file_handler, dict_path,
                 station_set_name, store, local_store, log):
        self.file_handler = file_handler
        self.dict_path = dict_path
        self.station_set_name = station_set_name
        self.store = store
        self.local_store = local_store
        self.log = log

    def get_dict(self, dict_folder, dict_name: str = None):
        if dict_name is None:
            dict_name = self.station_set_name
        file_path = os.path.join(self.dict_path, self.MODULE_FOLDER, self.STATIC_FOLDER, dict_folder, f"{dict_name}.json")
        static_dict = self.local_store.read(file_path)
        if static_dict is None:
            raise FileNotFoundError(f"[metadata_handler.get_dict] could not find {file_path}")
        return static_dict

    def get_station_info(self, dict_name: str = None):
        return self.get_dict(self.STATION_INFO_FOLDER, dict_name)

    def get_data_dict(self, dict_name: str = None):
        return self.get_dict(self.DATA_DICT_FOLDER, dict_name)

    def get_metadata(self, filename: str, store=None):
        return store.read(os.path.join(self.file_handler.relative_path, filename))

    def get_old_station_geo_metadata_by_store(self, station: str, store=None):
        if store is None:
            store = self.store
        old_metadata = self.get_metadata(f'{station}.geojson', store)
        return old_metadata

    def get_old_metadata_by_store(self, store=None):
        if store is None:
            store = self.store
        old_metadata = self.get_metadata(self.METADATA_FILE_NAME, store)
        return old_metadata

    def get_old_metadata(
            self
    ) -> dict:
        # Get old metadata using current store
        old_metadata = self.get_old_metadata_by_store()
        # get old metadata from local
        # if old_metadata is None:
        #     # Get old metadata using local_store (Look into processed_data folder)
        #     self.local_store.base_folder = self.file_handler.PROCESSED_DATA_ROOT
        #     old_metadata = self.get_old_metadata_by_store(self.local_store)

        if old_metadata is None:
            self.log.warn(f"[metadata_handler.get_old_metadata] could not find an old metadata")
        else:
            self.log.info(f"[metadata_handler.get_old_metadata] old metadata retrieved")
        return old_metadata

    def get_old_station_geo_metadata(
            self,
            station_id: str
    ) -> dict:
        # Get old station metadata using current store
        old_station_metadata = self.get_old_station_geo_metadata_by_store(station_id)

        # if old_station_metadata is None:
        #     # Get old station metadata using local_store (Look into processed_data folder)
        #     self.local_store.base_folder = self.file_handler.PROCESSED_DATA_ROOT
        #     old_station_metadata = self.get_old_station_geo_metadata_by_store(station_id, self.local_store)

        if old_station_metadata is None:
            self.log.warn(f"[metadata_handler.get_old_station_geo_metadata] could not find an old station metadata")
        else:
            self.log.info(f"[metadata_handler.get_old_station_geo_metadata] old station metadata retrieved")
        return old_station_metadata
