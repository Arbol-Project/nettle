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
