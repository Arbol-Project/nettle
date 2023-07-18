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
🚨 **UP FOR DEBATE** 🚨
Where constants are set, store is initialised, metadata is read in? (this includes metadata.json and stations.geojson)

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
The aim of the `transform()` step is to take that minimally modified data from `/raw_data` and transform it to a final format, ready to be saved to `/processed_data`. This final format is as such, with one temporal index column (dt) and a series of data columns:

| dt         | Var1    | Var2         |
|------------|---------|--------------|
| 1995-06-06 | fish    | strawberries |
| 1995-06-07 | chips   | cream        |
| 1995-06-08 | vinegar | pimms        |

#### Expected methods
I envision the method itself shouldn't need to be modified by the end user. It should however contain an abstractmethod, easily identifiable by the user, that deals with the transformation of a single station's data in to the final format. Sometimes data needs to be pulled from the raw data to use for metadata, so I imagine the parse method would look something like:

```python
def parse(self, **kwargs):
    for station in stations:
        t1 = time.time()
        try:
            # not edited per ETL
            raw_data = self.load_raw_data(station, **kwargs)
            # edited per ETL
            single_station_metadata, processed_data = self.transform_raw_data(raw_data, station, **kwargs)
            # not edited per ETL
            self.save_processed_data(processed_data, station, **kwargs)
            if bool(single_station_metadata):
                # likely edited per ETL but only necessary if metadata pulled from data files
                self.integrate_new_station_metadata(single_station_metadata, station_metadata, **kwargs)
            # not edited per ETL
            self.write_metadata(data, **kwargs)  
            t2 = time.time()
            self.log.info(
                f'Station_id={station_id} Time=\033[93m{(t2 - t1):.2f}\033[0m')
        except FailedStationException as se:
            self.log.error(
                f"Parse Station failed for {station_id}: {str(se)}")
    return
```

For datasets that require looking at their last update before 

#### Questions
-  Is this the right place to deal with metadata extraction from data? If handled in `extract()` which is being overwritten anyway, this could be cleaner.

-  What happens if data has no natural station-by-station approach?
-  Metadata has been glossed over slightly in this description, how does it fit in?
-  Does it make more sense to verify data whilst it's still in memory?

### verify()
### General gist
The point of this step is to make sure data and metadata are in the correct format. This includes but is not limited to:

-  Are date ranges continuous for datasets (e.g. 2021-02-01, 2021-02-02, 2021-02-03...)
-  Does `metadata.json` fit the desired format
-  Does `stations.geojson` fit the desired format and contain necessary fields. (🚨 **WHAT ARE THESE FIELDS** 🚨)

It is very unlikely that this step would require any modification by an ETL writer. As such its inner workings could remain pretty hidden.

### ~~add()~~ load()
### General gist
The final step is a simple one and should not be overwritten. If intending to output the files locally, this step is skipped. If outputting to s3, a simple copy of the `/processed_data/<path-to-current-update>` folder is all that is required. If using IPFS, this is handled by using the kubo rpc.


![-----------------------------------------------------](https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/rainbow.png)