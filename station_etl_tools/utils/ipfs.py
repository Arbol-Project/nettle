import dag_cbor
import requests
import io
import json
from multiformats import multicodec, multihash
from requests.adapters import HTTPAdapter, Retry

# Base methods

class IPFSIO:
    """
    Methods to be inherited by a DatasetManager that needs to instantiate and interact with an IPFS client
    """

    def __init__(
        self,
        host: str = "http://127.0.0.1:5001",
        default_hash: str
        | int
        | multicodec.Multicodec
        | multihash.Multihash = "sha2-256",
        default_base: str = "base32",
        default_timeout: int = 600,
    ):
        self._host = host
        self._default_base = default_base
        self._default_timeout = default_timeout
        self._default_hash = default_hash

        self.ipfs_session = IPFSIO.get_retry_session()

    # FUNDAMENTAL METHODS

    def ipfs_ls(self, cid: str):
        """
        List files in a directory

        :param cid:
        :return:
        """
        res = self.ipfs_session.post(
            self._host + "/api/v0/ls",
            timeout=self._default_timeout,
            params={"arg": str(cid)},
        )
        res.raise_for_status()
        return json.loads(res.content)['Objects'][0]['Links']

    def ipfs_add(self, file):
        res = self.ipfs_session.post(
            self._host + "/api/v0/add",
            params={
                "hash": self._default_hash
            },
            files={"dummy": file},
        )
        res.raise_for_status()

    def ipfs_cat(self, cid):
        res = self.ipfs_session.post(
            self._host + "/api/v0/cat",
            timeout=self._default_timeout,
            params={"arg": str(cid)},
        )
        res.raise_for_status()
        return json.loads(res.content)

    def ipfs_add_multiple_files_wrapping_with_directory(self, files_array):
        """

        :param files_array:
            Example: {'dummy0': <_io.BufferedReader name='file.txt'>, 'dummy1': <_io.BufferedReader name='file2.txt'>}
        :return:
            Returns directory hash of the files
        """
        files_dict = {}
        for idx, file in enumerate(files_array):
            files_dict[f'dummy{idx}'] = file

        res = self.ipfs_session.post(
            self._host + "/api/v0/add",
            params={
                "hash": self._default_hash,
                "wrap-with-directory": True
            },
            files=files_dict,
        )
        res.raise_for_status()
        resp = res.text.replace('\n', ',')
        resp_files_list = json.loads(f"[{resp.rsplit(',', 1)[0]}]")
        directory_dict = next((x for x in resp_files_list if x['Name'] == ''), None)
        if directory_dict is None:
            raise Exception('Could not create directory')

        return directory_dict['Hash']

    def ipfs_get(self, cid: str) -> dict:
        """
        Fetch a DAG CBOR object by its IPFS hash and return it as a JSON

        Parameters
        ----------
        cid : str
            The IPFS hash corresponding to a given object (implicitly DAG CBOR)

        Returns
        -------
        dict
            The referenced DAG CBOR object decoded as a JSON
        """
        res = self.ipfs_session.post(
            self._host + "/api/v0/block/get",
            timeout=self._default_timeout,
            params={"arg": str(cid)},
        )
        res.raise_for_status()
        # return res.content
        return dag_cbor.decode(res.content)

    def ipns_retrieve_object(self, key: str) -> tuple[dict, str, str] | None:
        """
        Retrieve a JSON object using its IPNS name key.

        Parameters
        ----------
        key : str
            The IPNS key string referencing a given object
        timeout : int, optional
            Time in seconds to wait for a response from `ipfs.name.resolve` and `ipfs.dag.get` before failing. Defaults to 30.

        Returns
        -------
        tuple[dict, str, str] | None
            A tuple of the JSON, the hash part of the IPNS key pair, and the IPFS/IPLD hash the IPNS key pair resolves
            to, or None if the object is not found
        """
        ipns_name_hash = self.ipns_key_list()[key]
        ipfs_hash = self.ipns_resolve(ipns_name_hash)
        json_obj = self.ipfs_get(ipfs_hash)
        return json_obj, ipns_name_hash, ipfs_hash

    def ipns_key_list(self) -> dict:
        """
        Return IPFS's Key List as a dict corresponding of key strings and associated ipns name hashes

        Returns
        -------
        ipns_key_hash_dict : dict
            All the IPNS name hashes and keys in the local IPFS repository
        """
        ipns_key_hash_dict = {}
        for name_hash_pair in self.ipfs_session.post(
            self._host + "/api/v0/key/list", timeout=self._default_timeout
        ).json()["Keys"]:
            key, val = tuple(name_hash_pair.values())
            ipns_key_hash_dict[key] = val
        return ipns_key_hash_dict

    def ipns_resolve(self, key: str) -> str:
        """
        Resolve the IPFS hash corresponding to a given key

        Parameters
        ----------
        key : str
            The IPNS key (human readable name) referencing a given dataset

        Returns
        -------
        str
            The IPFS hash corresponding to a given IPNS name hash
        """
        res = self.ipfs_session.post(
            self._host + "/api/v0/name/resolve",
            timeout=self._default_timeout,
            params={"arg": key},
        )
        res.raise_for_status()
        return res.json()["Path"][6:]  # 6: shaves off leading '/ipfs/'

    def ipfs_put(self, bytes_obj: bytes, should_pin: bool = True) -> str:
        """
        Turn a bytes object (file type object) into a DAG CBOR object compatible with IPFS and return its corresponding multihash

        Parameters
        ----------
        bytes_obj : bytes
            A file type (io.BytesIO) object to be converted into a DAG object and put on IPFS

        should_pin : bool, optional
            Whether to automatically pin this object when converting it to a DAG. Defauls to True.

        Returns
        -------
        str
            The IPFS hash (base32 encoded) corresponding to the newly created DAG object
        """
        res = self.ipfs_session.post(
            self._host + "/api/v0/dag/put",
            params={
                "store-codec": "dag-cbor",
                "input-codec": "dag-json",
                "pin": should_pin,
                "hash": self._default_hash,
            },
            files={"dummy": bytes_obj},
        )
        res.raise_for_status()
        return res.json()["Cid"]["/"]  # returns hash of DAG object created

    @staticmethod
    def json_to_bytes(obj: dict) -> bytes:
        """
        Convert a JSON object to a file type object (bytes). Primarily used for passing STAC metadata to IPFS

        Parameters
        ----------
        obj : dict
            The object (JSON) to be converted

        Returns
        -------
        bytes
            The json encoded as a file type object
        """
        return io.BytesIO(json.dumps(obj).encode("utf-8")).read()

    @staticmethod
    def csv_to_bytes(obj) -> bytes:
        return io.BytesIO(obj.encode('utf-8')).read()

    @staticmethod
    def get_retry_session() -> requests.Session:
        session = requests.Session()
        retries = Retry(connect=5, total=5, backoff_factor=4)
        session.mount("http://", HTTPAdapter(max_retries=retries))
        return session

if __name__ == "__main__":
    ip = IPFSIO()
    # ip.ipfs_ls("QmbZfhbeCsgNj6dyGWYUAbieundG8jhN6e5V5ZejMbDZs6")
    files = [open('file.txt', 'rb'), open('file2.txt', 'rb')]
    # ip.ipfs_add()
    ip.ipfs_add_multiple_files_wrapping_with_directory(files)