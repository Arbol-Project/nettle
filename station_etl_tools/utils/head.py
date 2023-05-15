import os
import json
import urllib
from . import settings


class Head:
    HEADS_FILE_NAME = "heads.json"
    HASHES_OUTPUT_ROOT = settings.HASHES_OUTPUT_ROOT
    HASH_HEADS_PATH = os.path.join(HASHES_OUTPUT_ROOT, HEADS_FILE_NAME)
    HISTORY_FILE_NAME = "history.json"
    HASH_HISTORY_PATH = os.path.join(HASHES_OUTPUT_ROOT, HISTORY_FILE_NAME)

    def __init__(self, ipfs_handler, http_root, log):
        self.ipfs_handler = ipfs_handler
        self.http_root = http_root
        self.log = log

    def heads(self):
        '''
        Return a dict of set names associated with head hash of the set
        '''
        if self.ipfs_handler.is_force_http_enabled():
            heads = self.heads_by_http()
        elif os.path.exists(self.HASH_HEADS_PATH):
            heads = self.heads_by_filesystem()
        else:
            heads = {}

        return heads

    # def heads_by_filesystem(self):
    #     '''
    #     Returns a JSON dict of the hashes in `HASH_HEADS_PATH`
    #     '''
    #     with open(self.HASH_HEADS_PATH) as fp:
    #         self.log.info(f"reading heads from {self.HASH_HEADS_PATH}")
    #         return json.load(fp)
    #
    # def heads_by_http(self):
    #     '''
    #     Get heads as JSON by connecting to a server at `self.http_root` which has a heads file at `self.HASH_HEADS_PATH`
    #     '''
    #     heads_url = f"{self.http_root}/{self.HEADS_FILE_NAME}"
    #     try:
    #         hashes = urllib.request.urlopen(heads_url)
    #     # Check for broken connection
    #     except (urllib.error.URLError, urllib.error.HTTPError):
    #         self.log.error(f"{heads_url} not reachable")
    #         raise
    #     # check for malformed JSON
    #     try:
    #         hashes_json = json.loads(hashes.read().decode("utf-8"))
    #     except json.decoder.JSONDecodeError:
    #         self.log.error(f"malformed JSON at {heads_url}")
    #         raise
    #     self.log.info(f"read heads from {heads_url}")
    #     return hashes_json
    #
    # def heads_history(self):
    #     '''
    #     Returns a JSON dict of the hashes in `HASH_HISTORY_PATH`
    #     '''
    #     with open(self.HASH_HISTORY_PATH) as fp:
    #         return json.load(fp)
    #
    # def write_head(self, root, key, history_key, include_in_history=True):
    #     '''
    #     Associate `key` with `root` by updating the JSON file at `HASH_HEADS_PATH`. If `key` is not set, it defaults to `self.json_key()`.
    #     If `include_in_history` also write to `HASH_HISTORY_PATH`.
    #     '''
    #
    #     heads = self.heads()
    #     heads[key] = root
    #     with open(self.HASH_HEADS_PATH, "w") as fp:
    #         json.dump(heads, fp, sort_keys=True, indent=4)
    #     self.log.info("wrote {} -> {} to {}".format(key,
    #                                                 root, self.HASH_HEADS_PATH))
    #     if include_in_history:
    #         history = self.heads_history()
    #         history[history_key] = root
    #         with open(self.HASH_HISTORY_PATH, "w") as fp:
    #             json.dump(history, fp, sort_keys=False, indent=4)
    #         self.log.info("wrote {} -> {} to {}".format(history_key,
    #                                                     root, self.HASH_HISTORY_PATH))
