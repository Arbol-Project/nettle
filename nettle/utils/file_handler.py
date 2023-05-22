import os
import glob
import json
import datetime
from . import settings
from .date_handler import DateHandler


class FileHandler:
    # paths relative to the script directory
    LOCAL_INPUT_ROOT = settings.LOCAL_INPUT_ROOT
    OUTPUT_ROOT = settings.OUTPUT_ROOT

    def __init__(self, custom_input_path, custom_output_path, climate_measurement_span,
                 date_with_time, relative_path):
        '''
        The file folder hierarchy for a set. This should be a relative path so it can be appended to other root paths like
        `self.local_input_path()` and `self.output_path()`
        '''
        self.relative_path = relative_path
        self.custom_input_path = custom_input_path
        self.custom_output_path = custom_output_path
        self.climate_measurement_span = climate_measurement_span
        self.date_with_time = date_with_time

    def local_input_path(self, local_input_root=LOCAL_INPUT_ROOT):
        '''
        The path to local data is built recursively by appending each derivative's relative path to the previous derivative's
        path. If a custom input path is set, force return the custom path.
        '''
        if self.custom_input_path:
            return self.custom_input_path

        path = os.path.join(local_input_root, self.relative_path)

        # Create directory if necessary
        if not os.path.exists(path):
            os.makedirs(path, 0o755, True)
        return path

    def existing_directories(self, root=None, sort=True):
        '''
        Get a list of directories present in specified root. If root is not specified, it will default to
        `self.OUTPUT_ROOT`/`self.relative_path()`. If sort is `True`, the listing will be sorted. This can be combined with the
        default behavior of the class to get a chronological list of folders available in the local output location of this set.
        '''
        # default to self.OUTPUT_ROOT/self.relative_path()
        if root is None:
            root = os.path.join(self.OUTPUT_ROOT, self.relative_path)
        existing = glob.glob(os.path.join(root, "[0-9]*"))
        if sort:
            existing = sorted(existing)
        return existing

    def last_local_output_directory(self, root=None):
        '''
        Return the last item in the sorting of `self.existing_directories()`. If root is specified, it will be passed to
        `self.existing_directories()`, otherwise `self.OUTPUT_ROOT`/`self.relative_path()` will be the root. Combined with
        the default behavior of this class, this function can be used to get the path to the most recent data available in
        the local output root.
        '''
        if self.existing_directories(root=root):
            return self.existing_directories(root=root)[-1]

    @staticmethod
    def load_dict(path):
        with open(path, encoding='utf-8') as f:
            return json.load(f)

    def get_folder_path_from_date(self, date, omit_root=False):
        '''
        Return a folder path inside `self.OUTPUT_ROOT` with the folder name based on `self.climate_measurement_span()`
        and the passed `datetime`. If `omit_root` is set, remove `self.OUTPUT_ROOT` from the path.
        '''
        if self.climate_measurement_span == DateHandler.SPAN_HOURLY:
            date_format = DateHandler.DATE_HOURLY_FORMAT_FOLDER
        else:
            date_format = DateHandler.DATE_FORMAT_FOLDER
        path = os.path.join(self.relative_path, date.strftime(date_format))
        if not omit_root:
            path = os.path.join(self.OUTPUT_ROOT, path)
        return path

    def output_path(self, omit_root=False):
        '''
        Return the path to a directory where parsed climate data will be written, automatically determining the end date and
        base on that. If `omit_root` is set, remove `self.OUTPUT_ROOT` from the path. Override with `self.custom_output_path`
        if that member variable is set
        '''
        if self.custom_output_path is not None:
            return self.custom_output_path
        else:
            # is this conversion necessary?
            date = datetime.datetime.fromisoformat(str(self.date_with_time))
            return self.get_folder_path_from_date(date, omit_root)

    def create_output_path(self):
        '''
        Make output directory
        '''
        os.makedirs(self.output_path(), 0o755, exist_ok=True)