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
from .ipfs import IPFSIO


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
        self.creds = Session(profile=credentials_name).get_credentials()
        self.base_folder = f"s3://{self.bucket}/"

    def __str__(self) -> str:
        return self.base_folder

    def fs(self, refresh: bool = False) -> s3fs.S3FileSystem:
        if refresh or not hasattr(self, "_fs"):
            try:
                self._fs = s3fs.S3FileSystem(
                    key=self.creds.access_key,
                    secret=self.creds.secret_key
                )
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


class IPFS(StoreInterface):
    HEADS_FILE_NAME = "heads.json"
    HISTORY_FILE_NAME = "history.json"
    HASHES_OUTPUT_ROOT = settings.HASHES_OUTPUT_ROOT
    HASH_HEADS_PATH = os.path.join(HASHES_OUTPUT_ROOT, HEADS_FILE_NAME)
    HASH_HISTORY_PATH = os.path.join(HASHES_OUTPUT_ROOT, HISTORY_FILE_NAME)

    def __init__(self, dataset_manager=None):
        super().__init__(dataset_manager)
        self.ipfs_io = IPFSIO()

    def has_existing_file(self, filepath) -> bool:
        pass

    def _read_hashes_file(self, path, encoding='utf-8'):
        self.dm.log.info(f"reading ipfs hash from {path}")
        with open(path, 'r', encoding=encoding) as f:
            return json.load(f)

    def _create_empty_hashes_file(self, path, encoding='utf-8'):
        self.dm.log.info(
            f"could not read hash file, creating a new one in {path}")
        # Create Directory
        try:
            os.makedirs(self.HASHES_OUTPUT_ROOT)
        except FileExistsError:
            # directory already exists
            pass

        # Create File with empty dict
        with open(path, 'w+', encoding=encoding) as f:
            json.dump({}, f, sort_keys=True, ensure_ascii=False, indent=4)
        return {}

    @staticmethod
    def _append_hash_in_hashes(hashes, content, key):
        hashes[key] = content
        return hashes

    def _append_hash_in_history_hashes(self, hashes, content):
        return self._append_hash_in_hashes(hashes, content, f"{self.dm}_{self.dm.today_with_time.date().strftime('%Y%m%d')}")

    def _append_hash_in_heads_hashes(self, hashes, content):
        return self._append_hash_in_hashes(hashes, content, f"{self.dm}")

    def _write_in_hashes_file(self, hash_ipfs, hash_path, encoding='utf-8'):
        with open(hash_path, "w", encoding=encoding) as fp:
            json.dump(hash_ipfs, fp, sort_keys=True,
                      ensure_ascii=False, indent=4)

    def _write_hash_in_hashes_file(self, directory_hash):
        try:
            heads_hashes = self._read_hashes_file(self.HASH_HEADS_PATH)
        except FileNotFoundError:
            try:
                heads_hashes = self._create_empty_hashes_file(
                    self.HASH_HEADS_PATH)
            except IOError as e:
                self.dm.log.error(
                    "I/O error({0}): {1}".format(e.errno, e.strerror))
                raise e
            except Exception as e:
                self.dm.log.error("Unexpected error writing heads hashes")
                raise e

        try:
            history_hashes = self._read_hashes_file(self.HASH_HISTORY_PATH)
        except FileNotFoundError:
            try:
                history_hashes = self._create_empty_hashes_file(
                    self.HASH_HISTORY_PATH)
            except IOError as e:
                self.dm.log.error(
                    "I/O error({0}): {1}".format(e.errno, e.strerror))
                raise e
            except Exception as e:
                self.dm.log.error("Unexpected error writing history hashes")
                raise e

        heads_hashes = self._append_hash_in_heads_hashes(
            heads_hashes, directory_hash)
        history_hashes = self._append_hash_in_history_hashes(
            history_hashes, directory_hash)

        self._write_in_hashes_file(heads_hashes, self.HASH_HEADS_PATH)
        self._write_in_hashes_file(history_hashes, self.HASH_HISTORY_PATH)

    def write(self, file_name: str, content, encoding='utf-8', **kwargs):
        # check if exist first
        # check if key exist

        # read locally in heads.json
        with open(self.HASH_HEADS_PATH, encoding=encoding) as fp:
            self.dm.log.info(f"reading ipfs hash from {self.HASH_HEADS_PATH}")
            hash_ipfs = json.load(fp)

        if isinstance(content, dict):
            hash_ipfs[f"{self.dm}_{self.dm.today_with_time.date()}_{file_name}"] = self.ipfs_io.ipfs_put(
                self.ipfs_io.json_to_bytes(content))
        elif isinstance(content, pd.DataFrame):
            print('content is dataframe')
            # jason = {'csv': self.ipfs_io.csv_to_bytes(content.to_csv(index=False))}
            jason = {'csv': content.to_csv(index=False)}

            # converts to string
            y = json.dumps(jason)

            # converts to bytes
            p = bytes(y, encoding="utf-8")
            hash_ipfs[f"{self.dm}_{self.dm.today_with_time.date()}_{file_name}"] = self.ipfs_io.ipfs_put(
                p)
            # hash_ipfs[f"{self.dm}_{self.dm.today_with_time.date()}_{file_name}"] = self.ipfs_io.ipfs_put(bytes(content.to_csv(index=False), encoding='utf-8'))
        else:
            raise Exception("Content file not identified")

        # write locally in heads.json
        with open(self.HASH_HEADS_PATH, "w", encoding=encoding) as fp:
            json.dump(hash_ipfs, fp, sort_keys=True,
                      ensure_ascii=False, indent=4)

    def read(self, cid, **kwargs):
        file_content = self.ipfs_io.ipfs_get(cid)
        return file_content

    def cat(self, cid, **kwargs):
        file_content = self.ipfs_io.ipfs_cat(cid)
        return file_content

    def list_directory_files(self, cid, **kwargs):
        return self.ipfs_io.ipfs_ls(cid)

    def latest_directory_hash(self, key, encoding='utf-8'):
        try:
            heads_hash = self._read_hashes_file(self.HASH_HEADS_PATH)
            return heads_hash[key]
        except (FileNotFoundError, IOError) as e:
            raise e
        except KeyError as e:
            raise e

    def json_key(self, append_date=False):
        '''
        Returns the key value that can identify this set in a JSON file. If `append_date` is True, add today's date to the end
        of the string
        '''
        return self.json_key_formatter(self.dm.name(), append_date)

    def json_key_formatter(self, name, append_date=False):
        import datetime
        key = "{}".format(name)
        if append_date:
            key = "{}{}{}".format(key, '_', datetime.datetime.now(
            ).strftime(self.dm.date_handler.DATE_FORMAT_FOLDER))

        return key

    def cp_local_folder_to_remote(self):
        local_path = self.dm.file_handler.output_path()

        try:
            # copy files to ipfs
            files = []
            for filename in os.listdir(local_path):
                heads_file = os.path.join(local_path, filename)
                files.append(open(heads_file, 'r'))
            directory_hash = self.ipfs_io.ipfs_add_multiple_files_wrapping_with_directory(
                files)
            self.dm.log.info(
                f"files created in IPFS with directory hash {directory_hash}")

            # set hashes in file
            # ToDo: Check if hashes/heads.json exist, if not create it with an empty dict
            self._write_hash_in_hashes_file(directory_hash)

            self.dm.log.info(
                f"directory hash written in {self.HASH_HEADS_PATH}")
        except IOError as e:
            self.dm.log.error(
                "I/O error({0}): {1}".format(e.errno, e.strerror))
            raise e
        except Exception as e:
            self.dm.log.error("Unexpected error writing station file")
            raise e

    def latest_hash(self):
        key = self.json_key()
        directory_cid = self.latest_directory_hash(key)
        return directory_cid

    # def latest_metadata(self, path, **kwargs):
    #     self.dm.log.info(f"getting latest metadata")
    #     directory_cid = self.latest_hash()
    #     files = self.list_directory_files(directory_cid)
    #     metadata_file = next(
    #         (file for file in files if file['Name'] == path), None)
    #     metadata_hash = metadata_file['Hash']
    #     if metadata_file is None:
    #         self.dm.log.warn(f"old metadata could not be found")
    #     else:
    #         self.dm.log.info(
    #             f"old metadata found in {path} on hash {metadata_hash}")
    #     return self.cat(metadata_hash)
