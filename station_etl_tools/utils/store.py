# This is necessary for referencing types that aren't fully imported yet. See https://peps.python.org/pep-0563/
from __future__ import annotations

import os
import s3fs
# import ipldstore
# import pathlib
import fsspec
from station_etl_tools.station_set import StationSet as dataset_manager
from abc import abstractmethod, ABC


class StoreInterface(ABC):

    def __init__(self, dm: dataset_manager):
        self.dm = dm

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


class S3(StoreInterface):

    def __init__(self, dm: dataset_manager, bucket: str):
        super().__init__(dm)
        if not bucket:
            raise ValueError("Must provide bucket name if parsing to S3")
        self.bucket = bucket

    def fs(self, refresh: bool = False) -> s3fs.S3FileSystem:
        if refresh or not hasattr(self, "_fs"):
            try:
                self._fs = s3fs.S3FileSystem(
                    key=os.environ["AWS_ACCESS_KEY_ID"],
                    secret=os.environ["AWS_SECRET_ACCESS_KEY"]
                    )
            except KeyError:  # KeyError indicates credentials have not been manually specified
                self._fs = s3fs.S3FileSystem()  # credentials automatically supplied from ~/.aws/credentials
            self.dm.log.info("Connected to S3 filesystem")
        return self._fs

    def file_outpath(self, file_name) -> str:
        outpath = os.path.join(self.folder_url,
                               self.dm.file_handler.output_path(omit_root=True),
                               file_name)
        return outpath

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
        outpath = os.path.join(self.folder_path, file_name)
        return outpath

    @property
    def folder_path(self) -> str:
        return self.dm.file_handler.output_path()

    def has_existing_file(self, file_name) -> bool:
        return os.path.exists(self.file_outpath(file_name))
