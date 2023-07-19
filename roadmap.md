<p align="center"> 
  <img src='docs/static/arbol.svg'></img>
</p>
<h1 align="center"> Nettle Roadmap </h1>

<h2> 🚧 Under Construction 🚧 </h2>

A large revamp of Nettle is underway. The aim of this is to vastly simplify the toolset **and** provide clarity on how to use it. This document is designed to both list the changes and act as a jumping off point for future relevant documentation.

![-----------------------------------------------------](https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/rainbow.png)

<h2> 🗺️ Ideal Structure 🗺️ </h2>

The point of this repo is to help users build ETLs. ETLs are defined as a series of steps to extract, transform and load data. For example a weather provider might provide rainfall data for London, UK via an API. A user could use Nettle to _programmatically_ ingest this data and save it locally, post it to S3 or store it on IPFS.

With the above in mind, there are four main functions Nettle believes constitutes an ETL. They are listed below with an eye to how we expect them to be used, and what functions they may contain.

### init()
Set the following constants, required unless specified:
-  COLLECTION (str) (eg. arbol, NOAA, speedwell, CME)
-  DATASET (str), <variables>-<frequency> (eg. temperature-daily, all-monthly, precipitation-hourly)
    -  Note that collection/dataset forms the default path for all outputs regardless of store
-  CUSTOM_OUTPUT_PATH (str = None)(not required), used in cases such as CME where we want outputs to look like `forecast/cme/ddif-daily`
-  multithread_transform (bool) (not required, default False) should the ETL be multithreaded at the transform stage?
-  DATA_DICTIONARY (dict) , used in transform to validate data output and used as a skeleton around which to build station-level metadata. Likely read from static files

### ~~update_local_input()~~ extract()
#### General gist
This involves downloading input from some external source and saving it locally. Although data processing is not encouraged at this point, some may be necessary to get your data saved locally in a station-by-station format. For example, if an API serves rain data for London, Cardiff and Edinburgh as a single file, this is the step where you would download that file, and save, to `/raw_data/<station_id>.csv`, the data for each city _separately_.

#### Expected methods
It is difficult to provide general tools for this step. The process of downloading data from external sources is often completely unique to the source. Nettle could provide a final step, likely replacing pd.to_csv() with some custom method that ensured data ended up in the right place. As the number of ETLs grows, we might notice that we are reusing the same code in this section over and over to, for example, download from FTP sites. If this is the case, that code could be generalised and 'demoted' to Nettle, but it seems to me a fool's errand to attempt this generalisation from the outset.

#### Expected output
By the end of `extract()`, the user should have saved locally, in `/raw_data`, the station-by-station information required to update their data source. Please note in this context a 'station' refers to some area with associated data. For example if your dataset was country-level population data, then your 'stations' would be countries. We use the word station because in general we are dealing with weather data from actual measuring stations.

The method `extract()` should return `True` if new data was found. This will trigger the next step of the process. If `False` is returned, it is assumed no new data was found and the process will end.

### ~~parse()~~ transform()
#### General gist
The aim of the `transform()` step is to take that minimally modified data from `/raw_data` and transform it to a final format, ready and saved to `/processed_data`. This final format is as such, with one temporal index column (dt) and a series of data columns:

| dt         | Var1    | Var2         |
|------------|---------|--------------|
| 1995-06-06 | fish    | strawberries |
| 1995-06-07 | chips   | cream        |
| 1995-06-08 | vinegar | pimms        |

#### Expected methods
I envision the method itself shouldn't need to be modified by the end user. It should however contain an abstractmethod, easily identifiable by the user, that deals with the transformation of a single station's data in to the final format. Sometimes data needs to be pulled from the raw data to use for metadata, so I imagine the parse method would look something like:

```python
def single_station_parse(self, station_id, **kwargs):
    raw_data = self.load_raw_data(station_id, **kwargs)
    # abstractmethod
    raw_station_metadata, processed_data = self.transform_raw_data(raw_data, station_id, **kwargs)
    # abstractmethod
    processed_station_metadata = self.transform_raw_metadata(raw_station_metadata, station_id, **kwargs)
    # the below are both verify steps and save steps
    # will need access to the global data dictionary, collection, dataset, custom_output_path
    self.save_processed_data(processed_data, station_id, **kwargs)
    self.save_processed_station_metadata(processed_station_metadata, station_id, **kwargs)

def transform(self, **kwargs):
    if self.multithread_transform:
        # multithreaded logic for single_station_parse
    else:
        stations = os.listdir('/raw_data/<collection>/<dataset>')
        # -4 cuts off .csv
        stations = [station[:-4] for station in stations if '.csv' in station]
        for station_id in stations:
            t1 = time.time()
            self.single_station_parse(station_id, **kwargs)
            t2 = time.time()
            self.log.info(
                f'Station_id={station_id} Time=\033[93m{(t2 - t1):.2f}\033[0m')
    # now construct/verify metadata.json and stations.geojson
    # this will have to download the latest versions of the metadata files
    # from somewhere
    self.save_combined_metadata_files(**kwargs)
    return
```

#### Expected output
By the end of `transform()`, the user should have saved locally, in `/processed_data`, all the data they wish to add to their store of choice, in the format they want it with relevant metadata.json, station-by-station geojson and a unified stations.geojson file.


#### Questions
**Q: What happens if data has no natural station-by-station approach?**

A: Stop using Nettle. In order to make useful tools we've had to make assumptions as to what the data looks like. Having station-by-station data, or some other natural equivalent (county, country, windfarm) is imperative to the nature of the beast.

### ~~add()~~ load()
### General gist
The final step is a simple one and should not be overwritten. If intending to output the files locally, this step is skipped. If outputting to s3, a simple copy of the `/processed_data/<path-to-current-update>` folder is all that is required. If using IPFS, this is handled by using the kubo rpc.

None of this should be surfaced in ETLs, it should just sit in Nettle and be ignored by ETL writers.


![-----------------------------------------------------](https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/rainbow.png)