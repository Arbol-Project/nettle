import datetime
import gzip
import ipfshttpclient
import json
import os
import settings
import sys
import urllib
from .date_handler import DateHandler


class IpfsHandler:
    HEADS_FILE_NAME = "heads.json"
    HASHES_OUTPUT_ROOT = settings.HASHES_OUTPUT_ROOT
    HASH_HEADS_PATH = os.path.join(HASHES_OUTPUT_ROOT, HEADS_FILE_NAME)
    HISTORY_FILE_NAME = "history.json"
    HASH_HISTORY_PATH = os.path.join(HASHES_OUTPUT_ROOT, HISTORY_FILE_NAME)
    FIELD_SEPARATOR = "-"
    IPNS_FILE_NAME = "ipns_record.json"
    IPNS_FILE_PATH = os.path.join(HASHES_OUTPUT_ROOT, IPNS_FILE_NAME)

    def __init__(self, force_http, log, http_root, custom_latest_hash,
                 station_set_name, climate_measurement_span):
        self.log = log
        self.force_filesystem_enabled = False
        self.force_http_enabled = force_http
        self.ipfs = None
        self.http_root = http_root
        self.custom_latest_hash = custom_latest_hash
        self.station_set_name = station_set_name
        self.climate_measurement_span = climate_measurement_span

    def content(self, full_path):
        return self.ipfs.cat(full_path)

    def connect_to_ipfs(self, force_reconnect=False):
        '''
        Use ipfshttpclient library to open a connection to IPFS. The original connection
        is intended to stay connected for the lifetime of this object, so it will only
        reconnect if there is no current connection or the `force_reconnect` flag is set.
        '''
        if self.ipfs is None or force_reconnect:
            self.ipfs = ipfshttpclient.connect(session=True, offline=False)

        return self.ipfs

    def add_json_to_ipfs(self, obj, compress=False, indent=2, encoding="utf-8", pin=True):
        '''
        If `compress` is set, convert `obj` to string and compress before adding.
        Otherwise, just use IPFS client `add_json` method to add `obj`.
        Return hash that was added to IPFS
        '''
        self.connect_to_ipfs()
        if not compress:
            link = self.ipfs.add_json(obj)
        else:
            json_string = json.dumps(obj, indent=indent)
            link = self.ipfs.add_bytes(
                gzip.compress(json_string.encode(encoding)))
        if not pin:
            self.ipfs.pin.rm(link)
        self.log.debug("added JSON to IPFS {}".format(link))
        return link

    def add_directory_to_ipfs(self, path, key, publish_to_ipns,
                              suppress_adding_to_heads=False, recursive=False):
        '''
        Add contents of directory at `path` to IPFS. If `path` is `None`, it defaults to `self.output_path()`. If
        `suppress_adding_to_heads` is `False`, store resulting hash in heads file, overwriting any hash already stored
        for this set. Recursive flag only has to be set if there are directories within the passed directory that also
        need to be added.
        '''
        if key is None:
            key = self.json_key()

        self.connect_to_ipfs()
        self.log.info(f"adding {path} to IPFS")
        try:
            # list of all added hashes
            added_hashes = self.ipfs.add(
                path, recursive=recursive, nocopy=True)
            # if add was successful, the last entry in the list will be the recursive hash containing everything in the
            # directory and will have the same name as our output path directory, otherwise the last entry will be some
            # file inside the directory
            if added_hashes and added_hashes[-1]["Name"] == os.path.basename(os.path.normpath(path)):
                set_hash = added_hashes[-1]["Hash"]
                self.log.info(f"received hash {set_hash}")
                if not suppress_adding_to_heads:
                    # if key is None:
                    #     key = self.json_key()
                    #     history_key = self.json_key(True)
                    # else:
                    #     history_key = f"{key}-{datetime.datetime.now()}"
                    self.write_head(set_hash, key)
                if publish_to_ipns:
                    if key is None:
                        key = self.json_key()
                    self.ipns_publish(key, set_hash)
                return set_hash
            else:
                raise ValueError(f"failed to build recursive hash of {path}")
        except:
            self.log.error(f"error adding {self} to IPFS: {sys.exc_info()}")
            raise

    @staticmethod
    def get_ipns_record(ipns_file_path):
        """
        Get a dict corresponding to the JSON with keys and their associated ipns names
        """
        try:
            with open(ipns_file_path) as f:
                return json.load(f)
        except FileNotFoundError:
            empty_record = {}
            with open(ipns_file_path, "w") as f:
                json.dump(empty_record, f)
            return empty_record

    def ipns_publish(self, key, set_hash, ipns_file_path):
        curr_ipns_record = self.get_ipns_record(ipns_file_path)
        if key not in curr_ipns_record:
            self.ipfs.key.gen(key, type="rsa")
        ipns_hash = self.ipfs.name.publish(
            f"/ipfs/{set_hash}", key=key, timeout=600)["Name"]
        self.log.info(
            f"Published {set_hash} for dataset {key} to {ipns_hash}")
        if key not in curr_ipns_record:
            self.write_ipns(ipns_hash, ipns_file_path, key=key)

    def write_ipns(self, ipns_hash, ipns_file_path, key=None):
        """
        Associate `ipns_hash` with `key` in the relevant file. Will only write if `key` not yet in the file
        """
        curr_ipns_record = self.get_ipns_record(ipns_file_path)
        if key not in curr_ipns_record:
            curr_ipns_record[key] = ipns_hash
            with open(ipns_file_path, "w") as fp:
                curr_ipns_record = json.dump(curr_ipns_record, fp)

    def unforce_filesystem(self):
        self.force_filesystem_enabled = False

    def is_force_filesystem_enabled(self):
        return self.force_filesystem_enabled

    # Doesn't seem to be used, maybe deprecate?
    # I think probably best to leave
    def force_filesystem(self):
        '''
        Force filesystem flag can be set to indicate that existing data for this dataset should be read from the filesystem instead of
        through the latest IPFS hash whenever possible (can be useful during testing before the set has been added to IPFS)
        '''
        self.unforce_http()
        self.force_filesystem_enabled = True
        self.log.info(
            "force filesystem enabled, filesystem calls will be used to retrieve data if possible")

    def force_http(self):
        '''
        Force HTTP flag can be set to indicate that existing data for this dataset should be fetched over HTTP instead of
        through the latest IPFS hash whenever possible
        '''
        self.unforce_filesystem()
        self.force_http_enabled = True
        self.log.info(
            "force HTTP enabled, HTTP calls will be used to retrieve data if possible")

    def unforce_http(self):
        self.force_http_enabled = False

    def is_force_http_enabled(self):
        return self.force_http_enabled

    def heads(self):
        '''
        Return a dict of set names associated with head hash of the set
        '''
        if self.is_force_http_enabled():
            heads = self.heads_by_http()
        elif os.path.exists(self.HASH_HEADS_PATH):
            heads = self.heads_by_filesystem()
        else:
            heads = {}

        return heads

    def heads_by_filesystem(self):
        '''
        Returns a JSON dict of the hashes in `HASH_HEADS_PATH`
        '''
        with open(self.HASH_HEADS_PATH) as fp:
            self.log.info(f"reading heads from {self.HASH_HEADS_PATH}")
            return json.load(fp)

    def heads_by_http(self):
        '''
        Get heads as JSON by connecting to a server at `self.http_root` which has a heads file at `self.HASH_HEADS_PATH`
        '''
        heads_url = f"{self.http_root}/{self.HEADS_FILE_NAME}"
        try:
            hashes = urllib.request.urlopen(heads_url)
        # Check for broken connection
        except (urllib.error.URLError, urllib.error.HTTPError):
            self.log.error(f"{heads_url} not reachable")
            raise
        # check for malformed JSON
        try:
            hashes_json = json.loads(hashes.read().decode("utf-8"))
        except json.decoder.JSONDecodeError:
            self.log.error(f"malformed JSON at {heads_url}")
            raise
        self.log.info(f"read heads from {heads_url}")
        return hashes_json

    def heads_history(self):
        '''
        Returns a JSON dict of the hashes in `HASH_HISTORY_PATH`
        '''
        with open(self.HASH_HISTORY_PATH) as fp:
            return json.load(fp)

    def write_head(self, root, key, include_in_history=True):
        '''
        Associate `key` with `root` by updating the JSON file at `HASH_HEADS_PATH`. If `key` is not set, it defaults to `self.json_key()`.
        If `include_in_history` also write to `HASH_HISTORY_PATH`.
        '''

        if key is None:
            key = self.json_key()
            history_key = self.json_key(True)
        else:
            history_key = f"{key}-{datetime.datetime.now()}"

        heads = self.heads()
        heads[key] = root
        with open(self.HASH_HEADS_PATH, "w") as fp:
            json.dump(heads, fp, sort_keys=True, indent=4)
        self.log.info("wrote {} -> {} to {}".format(key,
                                                    root, self.HASH_HEADS_PATH))
        if include_in_history:
            history = self.heads_history()
            history[history_key] = root
            with open(self.HASH_HISTORY_PATH, "w") as fp:
                json.dump(history, fp, sort_keys=False, indent=4)
            self.log.info("wrote {} -> {} to {}".format(history_key,
                                                        root, self.HASH_HISTORY_PATH))

    def hash_url(self, hash_str, link_name=""):
        '''
        Return URL to a hash + optional link stored on an IPFS HTTP gateway using `self.http_root` as the gateway
        '''
        return f"{self.http_root}/ipfs/{hash_str}/{link_name}"

    def contents_from_hash(self, hash_string, link_name="", as_json=False):
        '''
        Return the contents of the link `link_name` under the hash `recursive_hash` as a string. If `link_name` is empty,
        return the contents of the hash. If `as_json` is set, return the contents as a JSON object instead. If force HTTP
        is set in this class instance, the contents will be retrieved over HTTP through the gateway defined in `self.http_root`
        and returned as a bytes object. Otherwise, the IPFS client will be loaded and will try to retrieve the link contents.
        It doesn't make much sense to use this with filesystem forced, but in that case, the IPFS client will be used as well.
        '''
        # combine the hash and link
        full_path = os.path.join(hash_string, link_name)
        # get the contents from the HTTP gateway
        if self.is_force_http_enabled():
            target_url = self.hash_url(hash_string, link_name)
            self.log.info(
                f"retrieving contents of {full_path} over HTTP using URL {target_url}")
            try:
                with urllib.request.urlopen(target_url) as retrieved_file:
                    if as_json:
                        return json.loads(retrieved_file.read().decode("utf-8"))
                    else:
                        return retrieved_file.read()
            except (urllib.error.URLError, urllib.error.HTTPError):
                self.log.error(f"{target_url} not reachable")
                raise
        # get the contents using the IPFS HTTP client
        else:
            self.log.info(f"retrieving contents of {full_path} through IPFS")
            self.connect_to_ipfs()
            content = self.content(full_path)
            if as_json:
                # not using ipfshttpclient.Client.get_json because it's failing on one of our hashes
                return json.loads(content)
            else:
                return content

    def json_key_formatter(self, name, climate_measurement_span, append_date=False):
        key = "{}{}{}".format(name, self.FIELD_SEPARATOR,
                              climate_measurement_span)
        if append_date:
            key = "{}{}{}".format(key, self.FIELD_SEPARATOR, datetime.datetime.now(
            ).strftime(DateHandler.DATE_FORMAT_FOLDER))

        return key

    def json_key(self, append_date=False):
        '''
        Returns the key value that can identify this set in a JSON file. If `append_date` is True, add today's date to the end
        of the string
        '''
        return self.json_key_formatter(self.station_set_name,
                                       self.climate_measurement_span, append_date)

    def latest_hash(self, key=None):
        '''
        Returns the hash associated with the key in the heads list returned by `self.heads()` unless the latest hash has been overridden,
        in which case `self.custom_latest_hash` is returned. If key is not specified, it defaults to the key returned by `self.json_key()`.
        Returns `None` without error if no hash is found.
        '''
        if self.custom_latest_hash is not None:
            return self.custom_latest_hash
        if key is None:
            key = self.json_key()
        hashes = self.heads()
        if key in hashes:
            return hashes[key]
