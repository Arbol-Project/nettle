# station-climate-etl-ipfs
==============

station-climate-etl-ipfs is a set of utilities for retrieving publicly shared climate data, converting it to a common format, and adding it to your favorite storage medium, most notably [IPFS](https://ipfs.tech/). It is effectively a specialized web scraper for climate data that converts the data to a common format and lets you share it in a distributed fashion.

station-climate-etl-ipfs's utilities are combined in a StationSet abstract base class that can be adapted to retrieve data from a custom source. This abstract base class powers manager classes that can perform automated [data retrieval, transformation, and storage cycles](https://en.wikipedia.org/wiki/Extract,_transform,_load) (also known as ETLs) for a respective data source. The manager classes are also able to update, modify, and append to existing data in storage.

We have designed station-climate-etl-ipfs with a _very_ metadata-forward approach. The philosophy here is metadata in -> metadata out in a rigid way. Although this might appear to be overkill for retrieving CSVs from open data sources, it ensures that all data output with this tool is standardised. It also allows for a metadata catalogue that is machine-readable for easy accessibility and searchability of data, and hopefully a new standard of consistency across the world of climate data.

## How to use this repository
--------------------------

This repository provides a workflow for building climate data ETLs that output to IPFS, S3, or a local file system. This workflow utilizes a set of common methods in a predictable sequence to download raw data, transform it into a standard format, produce consistent metadata and finally write the overall dataset to the desired storage medium. If a dataset already exists in your specified storage medium it will either update or append new data to the set (as you request).

Users of this library should build ETLS for a desired non-gridded climate dataset by importing the library within an ETL manager script, using the `StationSet` class from [StationSet](station_etl_tools/station_set.py) as a base class, then applying its standardized workflow to the climate dataset in question. ETL child classes will need to overload one or many parent properties or methods from the [utils](station_etl_tools/utils) directory; the exact number depends on the intricacies of how the raw climate data is packaged. Where anticipated these methods are marked as **@abstractmethod** to prompt the user.

Users of this library can run the ETLs they build on the command line or within a notebook environment, as described below in [quickstart](#quickstart), When run, an ETL will first download raw data to a **datasets** directory and later output finalized data to a **climate** directory, creating either directory if they don't yet exist.


## Requirements
------------

* A Python >= 3.10 virtual environment for developing and running ETLs set up with the [required libraries](setup.cfg). It is likely that this repo will work for most machines with a python version earlier than this, but for apple silicon users we've set the lower bound at version 3.10.

* [IPFS 0.10+](https://github.com/ipfs/go-ipfs/) node **with a running daemon** (see [further instructions](docs/IPFS_Node_Management.md) for installation on a Linux machine). Note this is not required if you don't plan on uploading data to IPFS.


## Quickstart
----------

### Setup

First install the library from the github repository using `pip`. We recommend doing so within a Python virtual environment.

    pip install git+https://github.com/Arbol-Project/station-climate-etl-ipfs@<version number>

Eventually this should be available via pypy, and you'll simply pip install station-etl-tools directly.

Next install IPFS, if required, as per [the docs](docs/IPFS_Node_Management.md).

Once the library and an IPFS node are installed, instantiate an IPFS daemon. Open a terminal and run

    ipfs daemon &

Keep the terminal open as you move through the rest of the quickstart

### Running the ETL 🚧🚧🚧

With the IPFS daemon up and running manager scripts using the `station_etl_tools` library can be invoked within a separate script or notebook. Note you will have to first create a functioning manager script. There is an example of how to do this in the examples/managers folder of this repo, [here](examples/etls/managers/bom.py).

1) Copy the `etls` from this repo to a clean directory where you want to run your ETLs
2) Then copy and paste the below code in to a file at the same levels as `managers`, `static` and `climate` and run it!

``` python
from etls.managers.bom import BOM
from station_etl_tools.utils.store import Local, S3, IPFS
from station_etl_tools.utils import settings
import logging

logging.getLogger('').setLevel(logging.INFO)
ipfs_store = IPFS()
etl = BOM(log=logging.log, store=ipfs_store)
trigger_parse = BOM.update_local_input(etl)
perform_validation = BOM.parse(etl)
verified = BOM.verify(etl)
if verified:
    ipfs_store.cp_local_folder_to_remote()
```


### Retrieving your dataset 🚧🚧🚧

#### Local:
``` 
#{root}/climate/#{etl}/#{date} 
Example: ./station/climate/bom2/20230516 
``` 

#### S3:

```
aws s3 ls s3://#{S3_STATION_BUCKET}/datasets/bom2/#{date}/
Example: aws s3 ls s3://company-data/datasets/bom2/20230516/
```

#### IPFS:

check climate/hashes/heads.json for your most recent file and run
```
ipfs cat <hash>/<filename>
```
