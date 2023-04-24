import json
import os
from .metadata_handler import MetadataHandler
from .store import Local

class GeoJsonHandler:
    def __init__(self, file_handler, log):
        self.file_handler = file_handler
        self.log = log

    def write_geojson_to_file_with_geometry_info(self, geojson, data_manager, **kwargs):
        if 'store' not in kwargs:
            self.log.error('Store not setted')
            raise 'Store not setted'

        store = kwargs['store']
        filesystem = store.fs()
        outpath = store.file_outpath(
            MetadataHandler.STATION_METADATA_FILE_NAME.replace('json', 'geojson'))

        local_store = Local(dataset_manager=data_manager)
        local_filesystem = local_store.fs()
        local_outpath = local_store.file_outpath(
            MetadataHandler.STATION_METADATA_FILE_NAME.replace('json', 'geojson'))

        try:
            # Local
            with local_filesystem.open(local_outpath, 'w', encoding='utf-8') as fp:
                json.dump(geojson, fp, sort_keys=False,
                          ensure_ascii=False, indent=4)
            with filesystem.open(outpath, 'w', encoding='utf-8') as fp:
                json.dump(geojson, fp, sort_keys=False,
                          ensure_ascii=False, indent=4)
        except IOError as e:
            self.log.error("I/O error({0}): {1}".format(e.errno, e.strerror))
            raise e
        except Exception as e:
            self.log.error("Unexpected error writing station file")
            raise e

        self.log.info("wrote geojson metadata to {}".format(outpath))

    def write_geojson_to_file_without_geometry_info(self, geojson, data_manager, **kwargs):
        if 'store' not in kwargs:
            self.log.error('Store not setted')
            raise 'Store not setted'

        store = kwargs['store']
        filesystem = store.fs()
        outpath = store.file_outpath(MetadataHandler.STATION_METADATA_FILE_NAME)

        local_store = Local(dataset_manager=data_manager)
        local_filesystem = local_store.fs()
        local_outpath = local_store.file_outpath(
            MetadataHandler.STATION_METADATA_FILE_NAME)

        try:
            # Local
            with local_filesystem.open(local_outpath, 'w', encoding='utf-8') as fp:
                json.dump(geojson, fp, sort_keys=False,
                          ensure_ascii=False, indent=4)
            with filesystem.open(outpath, 'w', encoding='utf-8') as fp:
                json.dump(geojson, fp, sort_keys=False,
                          ensure_ascii=False, indent=4)
        except IOError as e:
            self.log.error("I/O error({0}): {1}".format(e.errno, e.strerror))
            raise e
        except Exception as e:
            self.log.error("Unexpected error writing station file")
            raise e

        self.log.info("wrote metadata to {}".format(outpath))

    def remove_geometry_from_geojson(self, geojson):
        # remove geometry to save space
        for i in range(len(geojson['features'])):
            geojson['features'][i].pop('geometry')

    # ToDo: Bug in feature
    def append_stations_not_in_new_update_to_metadata(self, old_station_metadata, old_stations, geojson):
        for old_feature in old_station_metadata['features']:
            if old_feature in old_stations:
                # maybe this should be old_feature['properties']['active hash'] = False
                feature['properties']['active hash'] = False
                geojson['features'].append(old_feature)

    @staticmethod
    def get_geometry(value):
        # append geometry
        # long lat data will be present if generated programatically
        if 'longitude' in value or 'latitude' in value:
            return {"type": "Point", "coordinates": [
                value['longitude'], value['latitude']]}
        # otherwise it should be included in the static station_info directory
        else:
            return value['geometry']

    @staticmethod
    def set_previous_hash(station_id, old_stations, old_hash, properties):
        # if the station_id was in our old metadata
        # then the previous hash is the current latest hash
        if station_id in old_stations:
            properties['previous hash'] = old_hash
            old_stations.remove(station_id)

    @staticmethod
    def base_properties(station_id):
        return {"station name": station_id}

    def get_properties(self, station_id, old_stations, old_hash, value):
        properties = self.base_properties(station_id)
        self.set_previous_hash(station_id, old_stations, old_hash, properties)

        for value_key in value:
            if value_key not in ['longitude', 'latitude', 'geometry']:
                properties[value_key] = value[value_key]

        return properties

    def append_features(self, geojson, old_stations, old_hash, station_ids):
        for station_id, value in station_ids:
            base_feature = {
                "type": "Feature",
                "geometry": self.get_geometry(value),
                "properties": self.get_properties(station_id, old_stations, old_hash, value)
            }

            # append to master geojson
            geojson["features"].append(base_feature)