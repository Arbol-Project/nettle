import os
import json
from nettle.utils import settings


class FileHandler:
    # paths relative to the script directory
    RAW_DATA_ROOT = settings.RAW_DATA_ROOT
    PROCESSED_DATA_ROOT = settings.PROCESSED_DATA_ROOT

    def __init__(self, relative_path):
        '''
        The file folder hierarchy for a set. This should be a relative path so it can be appended to other root paths like
        `self.local_input_path()` and `self.output_path()`
        '''
        self.relative_path = relative_path
        self.RAW_DATA_PATH = self.get_data_path(self.RAW_DATA_ROOT)
        self.PROCESSED_DATA_PATH = self.get_data_path(self.PROCESSED_DATA_ROOT)

    def create_directory_if_necessary(self, path):
        if not os.path.exists(path):
            os.makedirs(path, 0o755, True)

    def get_data_path(self, root):
        path = os.path.join(root, self.relative_path)
        self.create_directory_if_necessary(path)
        return path

    @staticmethod
    def load_dict(path):
        try:
            with open(path, encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return None

    # Check if any below here can be deleted

    # def local_input_path(self, local_input_root=RAW_DATA_ROOT):
    #     '''
    #     The path to local data is built recursively by appending each derivative's relative path to the previous derivative's
    #     path. If a custom input path is set, force return the custom path.
    #     '''
    #     if self.custom_raw_data_path:
    #         return self.custom_raw_data_path
    #
    #     path = os.path.join(local_input_root, self.relative_path)
    #
    #     # Create directory if necessary
    #     if not os.path.exists(path):
    #         os.makedirs(path, 0o755, True)
    #     return path
    #
    # def existing_directories(self, root=None, sort=True):
    #     '''
    #     Get a list of directories present in specified root. If root is not specified, it will default to
    #     `self.OUTPUT_ROOT`/`self.relative_path()`. If sort is `True`, the listing will be sorted. This can be combined with the
    #     default behavior of the class to get a chronological list of folders available in the local output location of this set.
    #     '''
    #     # default to self.OUTPUT_ROOT/self.relative_path()
    #     if root is None:
    #         root = os.path.join(self.PROCESSED_DATA_ROOT, self.relative_path)
    #     existing = glob.glob(os.path.join(root, "[0-9]*"))
    #     if sort:
    #         existing = sorted(existing)
    #     return existing
    #
    # def last_local_output_directory(self, root=None):
    #     '''
    #     Return the last item in the sorting of `self.existing_directories()`. If root is specified, it will be passed to
    #     `self.existing_directories()`, otherwise `self.OUTPUT_ROOT`/`self.relative_path()` will be the root. Combined with
    #     the default behavior of this class, this function can be used to get the path to the most recent data available in
    #     the local output root.
    #     '''
    #     if self.existing_directories(root=root):
    #         return self.existing_directories(root=root)[-1]
    #
    # def get_folder_path_from_date(self, date, omit_root=False):
    #     '''
    #     Return a folder path inside `self.OUTPUT_ROOT` with the folder name based on `self.climate_measurement_span()`
    #     and the passed `datetime`. If `omit_root` is set, remove `self.OUTPUT_ROOT` from the path.
    #     '''
    #     if self.climate_measurement_span == DateHandler.SPAN_HOURLY:
    #         date_format = DateHandler.DATE_HOURLY_FORMAT_FOLDER
    #     else:
    #         date_format = DateHandler.DATE_FORMAT_FOLDER
    #     path = os.path.join(self.relative_path, date.strftime(date_format))
    #     if not omit_root:
    #         path = os.path.join(self.PROCESSED_DATA_ROOT, path)
    #     return path
    #
    # def get_folder_path(self, omit_root=False):
    #     '''
    #     Return a folder path inside `self.OUTPUT_ROOT` with the folder name based on `self.climate_measurement_span()`
    #     and the passed `datetime`. If `omit_root` is set, remove `self.OUTPUT_ROOT` from the path.
    #     '''
    #     path = self.relative_path
    #     if not omit_root:
    #         path = os.path.join(self.PROCESSED_DATA_ROOT, path)
    #     return path
    #
    # def output_path(self, omit_root=False):
    #     '''
    #     Return the path to a directory where parsed climate data will be written, automatically determining the end date and
    #     base on that. If `omit_root` is set, remove `self.OUTPUT_ROOT` from the path. Override with `self.custom_output_path`
    #     if that member variable is set
    #     '''
    #     if self.custom_processed_data_path is not None:
    #         return self.custom_processed_data_path
    #     else:
    #         return self.get_folder_path(omit_root)
    #
    # def create_output_path(self):
    #     '''
    #     Make output directory
    #     '''
    #     os.makedirs(self.output_path(), 0o755, exist_ok=True)