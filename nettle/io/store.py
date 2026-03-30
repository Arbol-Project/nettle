# This is necessary for referencing types that aren't fully imported yet. See https://peps.python.org/pep-0563/
from __future__ import annotations

import os
import json
import s3fs
import fsspec
from contextlib import contextmanager
import pandas as pd
from botocore.session import Session
from abc import abstractmethod, ABC
from nettle.utils import settings


class StoreInterface(ABC):

    def __init__(self, log=None):
        self.log = log

    @classmethod
    def name(cls):
        '''
        Return the name of instantiated class
        '''
        return f"{cls.__name__}".lower()

    @abstractmethod
    def list_directory(self, path):
        pass

    @abstractmethod
    def has_existing_file(self, filepath: str) -> bool:
        pass

    @abstractmethod
    def write(self, filepath: str, content, encoding=None, **kwargs):
        pass

    @abstractmethod
    def read(self, filepath: str, file_type=None, **kwargs):
        pass

    @contextmanager
    def deal_with_errors(self, filepath):
        try:
            yield
        except FileNotFoundError as e:
            # Deal specifcally with FileNotFoundError?
            self.log.warn(
                "I/O error({0}): {1}. Filepath: {2}".format(e.errno, e.strerror, filepath))
            # raise e
        except IOError as e:
            self.log.warn(
                "I/O error({0}): {1}. Filepath: {2}".format(e.errno, e.strerror, filepath))
            # raise e
        except Exception as e:
            self.log.warn(
                "Unexpected error reading or writing file. Filepath: {}".format(filepath))
            raise e


class S3(StoreInterface):

    def __init__(
            self,
            log=None,
            bucket: str = '',
            credentials_name: str = ''
    ):
        super().__init__(log)
        self.bucket = bucket
        self.credentials_name = credentials_name
        self.creds = Session(profile=credentials_name).get_credentials() if credentials_name else None
        self.base_folder = f"s3://{self.bucket}/"

    def __str__(self) -> str:
        return self.base_folder

    def fs(self, refresh: bool = False) -> s3fs.S3FileSystem:
        if refresh or not hasattr(self, "_fs"):
            try:
                if self.creds is not None:
                    self._fs = s3fs.S3FileSystem(
                        key=self.creds.access_key,
                        secret=self.creds.secret_key
                    )
                else:
                    self._fs = s3fs.S3FileSystem()
            except KeyError:  # KeyError indicates credentials have not been manually specified
                self.log.error("[store.fs] s3 credentials not set")
            self.log.info("[store.fs] connected to S3 filesystem")
        return self._fs

    def list_directory(self, path: str):
        return self.fs().ls(path)

    @property
    def base_folder_without_s3(self):
        return f"{self.bucket}/"

    def has_existing_file(self, filepath: str) -> bool:
        """
        filepath = relative folder path + filename
        :param filepath:
        :return:
        """
        full_filepath = os.path.join(
            self.base_folder,
            filepath
        )
        return self.fs().exists(full_filepath)

    def has_existing_file_full_path(self, filepath):
        """
        filepath = s3 + bucket + relative folder path + filename
        :param filepath:
        :return:
        """
        return self.fs().exists(filepath)

    # relative_s3_path is the directory before the last directory
    # relative_s3_path usually is just collection name
    # For example, if you have this: s3://arbol-station-dev/bom2/bom2-daily/metadata.json
    # relative_s3_path would be: bom2
    def cp_folder_to_remote(self, local_path: str, relative_s3_path: str):
        s3_folder_path = os.path.join(self.base_folder, relative_s3_path)
        with self.deal_with_errors(local_path):
            if not self.has_existing_file_full_path(s3_folder_path):
                # Hack to create folder and avoid duplicated sub-folder
                self.fs().touch(os.path.join(s3_folder_path, 'tempCVG2Qy95Jp'))
                self.fs().put(local_path, s3_folder_path, recursive=True)
                self.fs().rm(os.path.join(s3_folder_path, 'tempCVG2Qy95Jp'))
            else:
                self.fs().put(local_path, s3_folder_path, recursive=True)
            return s3_folder_path

    def write(self, filepath: str, content, encoding=None, **kwargs):
        """
        filepath = relative folder path + filename
        :param filepath:
        :param content:
        :param encoding:
        :param kwargs:
        :return:
        """
        full_filepath = os.path.join(
            self.base_folder,
            filepath
        )

        with self.deal_with_errors(full_filepath):
            if isinstance(content, dict):
                encoding = 'utf-8'

            with self.fs().open(full_filepath, 'w', encoding=encoding) as f:
                if isinstance(content, dict):
                    json.dump(content, f, sort_keys=False,
                              ensure_ascii=False, indent=4)
                elif isinstance(content, pd.DataFrame):
                    content.to_csv(f, index=False)
                else:
                    raise Exception(
                        "[store.write] content file not identified")

        return filepath

    def read(self, filepath: str, file_type=None, **kwargs):
        if file_type is None:
            file_type = filepath.split(".")[-1]

        full_filepath = os.path.join(
            self.base_folder,
            filepath
        )

        with self.deal_with_errors(full_filepath):
            if self.has_existing_file_full_path(full_filepath):
                with self.fs().open(full_filepath, 'r') as f:
                    if file_type == 'csv':
                        csv = pd.read_csv(
                            f, dtype=str, on_bad_lines='skip')
                        return csv
                    elif file_type == 'json' or file_type == 'geojson':
                        return json.load(f)
                    else:
                        raise Exception(
                            '[store.read] file type not identified')

    # def latest_metadata(self, path: str, **kwargs):
    #     self.log.info(f"getting latest metadata")
    #     try:
    #         if self.custom_latest_metadata_path:
    #             directory = self.custom_latest_metadata_path
    #         else:
    #             directory = self.latest_directory()
    #         file = f"{directory}/{path}"
    #         metadata_file = self.read(file)
    #     except IndexError:
    #         metadata_file = None
    #
    #     if metadata_file is None:
    #         self.log.warn(f"old metadata could not be found")
    #     else:
    #         self.log.info(f"old metadata found in {file}")
    #     return metadata_file


class Local(StoreInterface):
    def __init__(
            self,
            log=None,
            base_folder: str = ''
    ):
        super().__init__(log)
        self.base_folder = base_folder

    def __str__(self) -> str:
        return self.base_folder

    def fs(self, refresh: bool = False) -> fsspec.implementations.local.LocalFileSystem:
        if refresh or not hasattr(self, "_fs"):
            self._fs = fsspec.filesystem("file")
        return self._fs

    def list_directory(self, path: str):
        # ToDo
        return None

    def has_existing_file(self, filepath: str) -> bool:
        return self.fs().exists(filepath)
        # full_filepath = os.path.join(
        #     self.base_folder,
        #     filepath
        # )
        # return os.path.exists(full_filepath)

    def has_existing_file_full_path(self, filepath):
        """
        filepath = s3 + bucket + relative folder path + filename
        :param filepath:
        :return:
        """
        return self.fs().exists(filepath)

    def write(self, filepath: str, content, encoding=None, **kwargs):
        full_filepath = os.path.join(
            self.base_folder,
            filepath
        )

        with self.deal_with_errors(full_filepath):
            if isinstance(content, dict):
                encoding = 'utf-8'
            with self.fs().open(full_filepath, 'w', encoding=encoding) as f:
                if isinstance(content, dict):
                    json.dump(content, f, sort_keys=False,
                              ensure_ascii=False, indent=4)
                elif isinstance(content, pd.DataFrame):
                    content.to_csv(f, index=False)
                else:
                    # make this a better error
                    raise Exception(
                        "[store.write] content file not identified")

        return filepath

    def read(self, filepath: str, file_type=None, **kwargs):
        if file_type is None:
            file_type = filepath.split(".")[-1]

        full_filepath = os.path.join(
            self.base_folder,
            filepath
        )

        with self.deal_with_errors(full_filepath):
            if self.has_existing_file_full_path(full_filepath):
                with self.fs().open(full_filepath, 'r') as f:
                    if file_type == 'csv':
                        csv = pd.read_csv(f, dtype=str, na_values="")
                        return csv
                    elif file_type == 'json' or file_type == 'geojson':
                        return json.load(f)
                    else:
                        raise Exception(
                            '[store.read] file type not identified')

    # def metadata_by_filesystem(self, directory, path):
    #     '''
    #     Get metadata from local filesystem by passing in a root folder path
    #     '''
    #     metadata_path = os.path.join(directory, path)
    #     self.dm.log.info(f"getting metadata from {metadata_path}")
    #     if os.path.exists(metadata_path):
    #         with open(metadata_path, "rt") as metadata:
    #             return json.load(metadata)
    #     else:
    #         self.dm.log.warn(f"no metadata file found at {metadata_path}")
    #
    # def latest_metadata(self, path, **kwargs):
    #     self.dm.log.info(f"getting latest metadata")
    #     if self.custom_latest_metadata_path:
    #         directory = self.custom_latest_metadata_path
    #     else:
    #         directory = self.folder_path
    #     metadata_file = self.metadata_by_filesystem(
    #         directory=directory, path=path)
    #     if metadata_file is None:
    #         self.dm.log.warn(f"old metadata could not be found")
    #     else:
    #         self.dm.log.info(f"old metadata found")
    #     return metadata_file


