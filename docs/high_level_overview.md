<p align="center"> 
  <img src='docs/static/arbol.svg'></img>
</p>
<h1 align="center"> 🌱 Nettle 🌱 </h1>

<h2> Structure 🏗️ </h2>

The point of this repo is to help users build ETLs. ETLs are defined as a series of steps to extract, transform and load data. For example a weather provider might provide rainfall data for London, UK via an API. A user could use Nettle to _programmatically_ ingest this data and save it locally, post it to S3 or store it on IPFS.

With the above in mind, there are four main functions Nettle believes constitutes an ETL. They are listed below with an eye to how we expect them to be used, and what functions they may contain.

### init() ▶️
Set the following constants, required unless specified:
-  collection (str) - Where does the data come from? Should be lowercase unless an acronym, which is uppercase (eg. arbol, NOAA, speedwell, CME)
-  dataset (str) - `<variables>-<frequency>` (eg. temperature-daily, all-monthly, precipitation-hourly)
    -  Note that collection/dataset forms the default path for all outputs regardless of store (eg. NOAA/ghcn-daily, speedwell/temperature-daily)
-  custom_relative_data_path (str = None) - used in cases such as CME where we want outputs to look like `forecast/cme/ddif-daily`
-  multithread_transform (bool = None) - should the ETL be multithreaded at the transform stage?

There are other constants defined for you in `init()`. These are often self explanatory but an ever growing list of explanations can be found here:
-  date_range_handler, file_handler, metadata_handler - Helper classes to handle various aspects of date management and file io.
-  self.DATA_DICTIONARY (dict) - The data dictionary of all columns possible in your output dataset. It is very unlikely you'll be able to write an ETL without this.
-  self.STATION_DICTIONARY (dict) - Sometimes static info about stations will need to be passed to your ETL. This is where you should put that info. Note this isn't always essential, but is useful if you only want to pull a subset of stations, or if you have extra station info that can't be accessed programmatically from the data itself.

In the nebulous space between `init()` and `extract()`, it is useful (read: imperative) to define two abstract methods:
-  default_metadata() - Populate the default values for metadata. It is very likely that this only runs the first time you run an ETL.
-  default_station_metadata(station_id) - Populate the default values for single station metadata. It is very likely that this only runs the first time you run an ETL or when you find a new station.

### extract() ⛏️
#### General gist
This involves downloading input from some external source and saving it locally. Although data processing is not encouraged at this point, some may be necessary to get your data saved locally in a station-by-station format. For example, if an API serves rain data for London, Cardiff and Edinburgh as a single file, this is the step where you would download that file, and save, to `/raw_data/<station_id>.csv`, the data for each city _separately_.

#### Expected methods
It is difficult to provide general tools for this step. The process of downloading data from external sources is often completely unique to the source. Nettle could provide a final step, likely replacing pd.to_csv() with some custom method that ensured data ended up in the right place. As the number of ETLs grows, we might notice that we are reusing the same code in this section over and over to, for example, download from FTP sites. If this is the case, that code could be generalised and 'demoted' to Nettle, but it seems to me a fool's errand to attempt this generalisation from the outset.

#### Expected output
By the end of `extract()`, the user should have saved locally, in `/raw_data`, the station-by-station information required to update their data source. Please note in this context a 'station' refers to some area with associated data. For example if your dataset was country-level population data, then your 'stations' would be countries. We use the word station because in general we are dealing with weather data from actual measuring stations.

The method `extract()` should return `True` if new data was found. This will trigger the next step of the process. If `False` is returned, it is assumed no new data was found and the process will end.

### transform() 🪄
#### General gist
The aim of the `transform()` step is to take that minimally modified data from `/raw_data` and transform it to a final format, ready and saved to `/processed_data`. This final format is as such, with one temporal index column (dt) and a series of data columns:

| dt         | Var1    | Var2         |
|------------|---------|--------------|
| 1995-06-06 | fish    | strawberries |
| 1995-06-07 | chips   | cream        |
| 1995-06-08 | vinegar | pimms        |

#### Expected methods
I envision the method itself shouldn't need to be modified by the end user. It does however contain two abstractmethods:
-  transform_raw_data() - Used for reading the raw station data file and transforming it to the final format. This is also where you can pull metadata out from the data file for use in the next abstract method
-  transform_raw_metadata() - Used for combining the useful info you pulled out in the previous step and combining it with some base version.

#### Expected output
By the end of `transform()`, the user should have saved locally, in `/processed_data`, all the data they wish to add to their store of choice, in the format they want it with relevant metadata.json, station-by-station geojson and a unified stations.geojson file.


#### Questions
**Q: What happens if data has no natural station-by-station approach?**

A: Stop using Nettle. In order to make useful tools we've had to make assumptions as to what the data looks like. Having station-by-station data, or some other natural equivalent (county, country, windfarm) is imperative to the nature of the beast.

### load() 🚀
### General gist
You have the flexibility to load data as you see fit. However we intend for the load step to look something like:
-  Local store - nothing happens, you've already saved the data to `processed_data/collection/dataset/...`
-  S3 - copy the local folder to an s3 bucket of your choosing using `cp_folder_to_remote` in `nettle/io/store.py`
-  IPFS - copy the local folder to your configured IPFS environment using `cp_local_folder_to_remote` in `nettle/io/store.py`


![-----------------------------------------------------](https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/rainbow.png)