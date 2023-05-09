# This is necessary for referencing types that aren't fully imported yet. See https://peps.python.org/pep-0563/
from __future__ import annotations

import os
import json
import s3fs
# import ipldstore
import pathlib
import fsspec
from abc import abstractmethod, ABC
from . import settings
from .ipfsio import IPFSIO
import pandas as pd


class StoreInterface(ABC):

    def __init__(self, dataset_manager):
        self.dm = dataset_manager

    # @abstractmethod
    # def mapper(self, **kwargs: dict) -> collections.abc.MutableMapping:
    #     pass

    @abstractmethod
    def has_existing_file(self, station_id) -> bool:
        pass

    # def dataset(self, **kwargs: dict) -> xr.Dataset | None:
    #     if self.has_existing:
    #         return xr.open_zarr(self.mapper(**kwargs))
    #     else:
    #         return None

    @abstractmethod
    def write(self, file_name: str, content, encoding, **kwargs):
        pass

    @abstractmethod
    def read(self, **kwargs):
        pass


class S3(StoreInterface):

    def __init__(self, dataset_manager, bucket: str):
        super().__init__(dataset_manager)
        if not bucket:
            raise ValueError("Must provide bucket name if parsing to S3")
        self.bucket = bucket

    def fs(self, refresh: bool = False) -> s3fs.S3FileSystem:
        if refresh or not hasattr(self, "_fs"):
            try:
                self._fs = s3fs.S3FileSystem(
                    key=settings.AWS_ACCESS_KEY,
                    secret=settings.AWS_SECRET_KEY
                    )
            except KeyError:  # KeyError indicates credentials have not been manually specified
                self._fs = s3fs.S3FileSystem()  # credentials automatically supplied from ~/.aws/credentials
            self.dm.log.info("Connected to S3 filesystem")
        return self._fs

    def file_outpath(self, file_name) -> str:
        return os.path.join(
            self.folder_url,
            self.dm.file_handler.output_path(omit_root=True),
            file_name
        )

    def folder_outpath(self) -> str:
        return os.path.join(
            self.folder_url,
            self.dm.file_handler.output_path(omit_root=True)
        )

    @property
    def folder_url(self) -> str:
        return f"s3://{self.bucket}/station-test/datasets/"

    def __str__(self) -> str:
        return self.folder_url

    # def mapper(self, refresh: bool = False, **kwargs: dict) -> fsspec.mapping.FSMap:
    #     if refresh or not hasattr(self, "_mapper"):
    #         self._mapper = s3fs.S3Map(root=self.url, s3=self.fs())
    #     return self._mapper

    def has_existing_file(self, file_name) -> bool:
        return self.fs().exists(self.file_outpath(file_name))

    def cp_local_folder_to_remote(self):
        filesystem = self.fs()
        local_path = self.dm.file_handler.output_path()
        s3_path = self.folder_outpath()

        try:
            filesystem.put(local_path, s3_path, recursive=True)
        except IOError as e:
            self.dm.log.error("I/O error({0}): {1}".format(e.errno, e.strerror))
            raise e
        except Exception as e:
            self.dm.log.error("Unexpected error writing station file")
            raise e

    def write(self, file_name: str, content, encoding=None, **kwargs):
        pass

    def write_file(self, file_name: str, content, encoding=None, **kwargs):
        filesystem = self.fs()
        filepath = self.file_outpath(file_name)

        try:
            with filesystem.open(filepath, 'w', encoding=encoding) as f:
                if isinstance(content, dict):
                    json.dump(content, f, sort_keys=False, ensure_ascii=False, indent=4)
                elif isinstance(content, pd.DataFrame):
                    content.to_csv(f, index=False)
                else:
                    # make this a better error
                    raise Exception("Content file not identified")
        except IOError as e:
            self.dm.log.error("I/O error({0}): {1}".format(e.errno, e.strerror))
            raise e
        except Exception as e:
            self.dm.log.error("Unexpected error writing station file")
            raise e

        return filepath

    def read(self, filepath: str, file_type=None):
        if file_type is None:
            file_type = filepath.split(".")[-1]

        try:
            if file_type == 'csv':
                csv = pd.read_csv(filepath)
                return csv
            else:
                # ToDo: make a better error
                raise Exception('Could not identify file type')
        except FileNotFoundError:
            # warning logged in StationSet get_historical_dataframe
            return None


class Local(StoreInterface):
    def fs(self, refresh: bool = False) -> fsspec.implementations.local.LocalFileSystem:
        if refresh or not hasattr(self, "_fs"):
            self._fs = fsspec.filesystem("file")
        return self._fs

    # def mapper(self, refresh=False, **kwargs) -> fsspec.mapping.FSMap:
    #     if refresh or not hasattr(self, "_mapper"):
    #         self._mapper = self.fs().get_mapper(self.path)
    #     return self._mapper

    def __str__(self) -> str:
        return str(self.folder_path)

    def file_outpath(self, file_name) -> str:
        return os.path.join(self.folder_path, file_name)

    @property
    def folder_path(self) -> str:
        return self.dm.file_handler.output_path()

    def has_existing_file(self, file_name) -> bool:
        return os.path.exists(self.file_outpath(file_name))

    def write(self, file_name: str, content, encoding=None, **kwargs):
        filesystem = self.fs()

        filepath = self.file_outpath(file_name)

        try:
            with filesystem.open(filepath, 'w', encoding=encoding) as f:
                if isinstance(content, dict):
                    json.dump(content, f, sort_keys=False, ensure_ascii=False, indent=4)
                elif isinstance(content, pd.DataFrame):
                    content.to_csv(f, index=False)
                else:
                    # make this a better error
                    raise Exception("Content file not identified")
        except IOError as e:
            self.dm.log.error("I/O error({0}): {1}".format(e.errno, e.strerror))
            raise e
        except Exception as e:
            self.dm.log.error("Unexpected error writing station file")
            raise e

        return filepath

    def read(self):
        pass


class IPFS(StoreInterface):
    HEADS_FILE_NAME = "heads.json"
    HASHES_OUTPUT_ROOT = settings.HASHES_OUTPUT_ROOT
    HASH_HEADS_PATH = os.path.join(HASHES_OUTPUT_ROOT, HEADS_FILE_NAME)

    def __init__(self, dataset_manager):
        super().__init__(dataset_manager)
        self.ipfs_io = IPFSIO()

    def has_existing_file(self, station_id) -> bool:
        pass

    def file_outpath(self, file_name) -> str:
        return 'bafyreihyrqbkboajigznlhhf2g7vuyzmvag7mclighw5e3tu4n3zewfu4i'

    def _read_hashes_file(self, encoding='utf-8'):
        with open(self.HASH_HEADS_PATH, encoding=encoding) as fp:
            self.dm.log.info(f"reading ipfs hash from {self.HASH_HEADS_PATH}")
            return json.load(fp)

    def _append_hash_in_hashes_file(self, hashes, content):
        hashes[f"{self.dm}_{self.dm.today_with_time.date()}"] = content
        return hashes

    def _write_in_hashes_file(self, hash_ipfs, encoding='utf-8'):
        with open(self.HASH_HEADS_PATH, "w", encoding=encoding) as fp:
            json.dump(hash_ipfs, fp, sort_keys=True, ensure_ascii=False, indent=4)

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
            hash_ipfs[f"{self.dm}_{self.dm.today_with_time.date()}_{file_name}"] = self.ipfs_io.ipfs_put(p)
            # hash_ipfs[f"{self.dm}_{self.dm.today_with_time.date()}_{file_name}"] = self.ipfs_io.ipfs_put(bytes(content.to_csv(index=False), encoding='utf-8'))
        else:
            # make this a better error
            raise Exception("Content file not identified")

        # write locally in heads.json
        with open(self.HASH_HEADS_PATH, "w", encoding=encoding) as fp:
            json.dump(hash_ipfs, fp, sort_keys=True, ensure_ascii=False, indent=4)

    def read(self, **kwargs):
        print('read')
        file = self.ipfs_io.ipfs_get('bafyreihyrqbkboajigznlhhf2g7vuyzmvag7mclighw5e3tu4n3zewfu4i')
        # file = ip.ipns_retrieve_object('bafyreihyrqbkboajigznlhhf2g7vuyzmvag7mclighw5e3tu4n3zewfu4i')
        print(file)

    def cp_local_folder_to_remote(self):
        local_path = self.dm.file_handler.output_path()

        try:
            files = []
            for filename in os.listdir(local_path):
                heads_file = os.path.join(local_path, filename)
                files.append(open(heads_file, 'r'))
            directory_hash = self.ipfs_io.ipfs_add_multiple_files_wrapping_with_directory(files)
            self.dm.log.info(f"files created in IPFS with directory hash {directory_hash}")
            # ToDo: Check if hashes/heads.json exist, if not create it with an empty dict
            # ToDo: append directory_hash to hashes/heads.json
            hashes = self._read_hashes_file()
            hashes = self._append_hash_in_hashes_file(hashes, directory_hash)
            self._write_in_hashes_file(hashes)
            self.dm.log.info(f"directory hash written in {self.HASH_HEADS_PATH}")
        except IOError as e:
            self.dm.log.error("I/O error({0}): {1}".format(e.errno, e.strerror))
            raise e
        except Exception as e:
            self.dm.log.error("Unexpected error writing station file")
            raise e