import pandas as pd
from datetime import datetime, timedelta, date
from dateutil.rrule import rrule, MONTHLY
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from nettle.station_set import StationSet


class BOMTest(StationSet):
    @staticmethod
    def collection() -> str:
        return "BOMTest"

    @staticmethod
    def dataset() -> str:
        return "bom-test-daily"

    def fill_in_static_metadata(self, base_metadata: dict, **kwargs) -> dict:
        base_metadata['name'] = "BOM Test Weather Station Data"
        base_metadata['data source'] = "http://www.bom.gov.au/"
        base_metadata['documentation'] = "Weather data from the Bureau of Meteorology in Australia covering roughly " \
            "500 stations"
        base_metadata['tags'] = ["temperature", "rain", "wind", "australia"]
        base_metadata['data dictionary'] = self.DATA_DICTIONARY
        base_metadata["previous hash"] = self.store.latest_hash() if self.store.name() == 'ipfs' else None
        base_metadata["time generated"] = str(self.today_with_time)
        return base_metadata

    def transform_raw_data(self, base_station_metadata: dict, raw_dataframe: pd.DataFrame, station_id: str, **kwargs) -> tuple[dict, pd.DataFrame]:
        raw_dataframe.drop_duplicates(subset=['dt'], ignore_index=True, inplace=True)
        return base_station_metadata, raw_dataframe

    def transform_raw_metadata(self, raw_station_metadata: dict, station_id: str, **kwargs) -> dict:
        raw_station_metadata['features'][0]['geometry'] = self.STATION_DICTIONARY[f'{station_id}']['geometry']
        raw_station_metadata['features'][0]['properties']["station name"] = f"{station_id}"
        raw_station_metadata['features'][0]['properties']["code"] = self.STATION_DICTIONARY[f'{station_id}']['code']
        raw_station_metadata['features'][0]['properties']["file name"] = f"{self.station_name_formatter(station_id)}.csv"
        return raw_station_metadata

    def extract(self):
        stations_updated_counter = 0
        stations = list(self.STATION_DICTIONARY.keys())
        for station_id in stations:
            with self.etl_print_runtime(station_id, 'extract'), self.check_station_extract_loop(station_id):
                new_start = self.get_new_start(station_id)
                station_df = self.get_dfs_with_formatted_data(new_start=new_start, station_id=station_id)
                self.save_raw_dataframe(station_df, station_id)
                stations_updated_counter += int(pd.Timestamp(new_start) > station_df["dt"].max())
        return stations_updated_counter != len(stations)

    def get_new_start(self, station_id: str):
        if "date range" in self.STATION_DICTIONARY[station_id]:
            # previous end date + 1 days
            return self.STATION_DICTIONARY[station_id]["date range"][1].date() + timedelta(days=1)
        else:
            new_start = self.today_with_time.date() + timedelta(weeks=-55)  # Data available up to 14 months back
            return new_start.replace(day=1)  # hard code day to the 1st of the month

    def get_dfs_with_formatted_data(self, new_start: date, station_id: str):
        url_id = self.STATION_DICTIONARY[station_id]["code"]
        dfs = []
        # Need to replace day with 1st of month to avoid skipping months with rrule
        for dt_month in rrule(MONTHLY, dtstart=new_start.replace(day=1), until=self.today_with_time.replace(day=1)):
            dfs.append(self.single_df(str(f"{dt_month.month:02}"), str(dt_month.year), url_id))
        station_df = pd.concat(dfs, ignore_index=True)
        station_df['dt'] = [datetime.strptime(x, '%Y-%m-%d') for x in station_df['dt']]  # index
        station_df.sort_values(by='dt', inplace=True, ignore_index=True)
        return station_df

    def single_df(self, month: str, year: str, url_id: str):
        """
        Given a month, year and ID, return all the climate data for that month. Will return an empty dataframe
        with columns ['Date','Tmin','Tmax','Rain','Winddir','Windspeed'] if anything goes wrong
        """
        try:
            new_url = f'http://www.bom.gov.au/climate/dwo/{year+month}/text/{url_id}.{year+month}.csv'
            req = Request(new_url)
            req.add_header('User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0')
            raw_rows = str(urlopen(req).read()).split('\\r\\n')
            rows = []
            # There is some intro guff until empty line '', Start reading from after this line onwards
            for i in range(raw_rows.index('') + 1, len(raw_rows)):
                row = raw_rows[i][1:].split(',')  # Ignore first character it's always a space
                row = [x.replace("'", "").replace('"', '') for x in row]  # Replace existing apostrophes/speech marks
                rows.append(row)

            df = pd.DataFrame(columns=rows[0], data=rows[1:])
            cols = ['Date']
            # Columns contain unreferenceable char for degrees c iterate through and get column names from df instead
            for column in df.columns:
                [cols.append(column) for key in ['maximum', 'minimum', 'rainfall'] if (key in column.lower())]

            # 1 - Last column is time of max wind gust, remove and use cols to select the columns we want
            # 2 - Rename to avoid confusing unreferenceable character situation
            return df[cols[:-1]].set_axis(['dt', 'TMIN', 'TMAX', 'RAIN', 'WINDDIR', 'WINDSPEED'], axis="columns")
        except HTTPError:
            self.log.info(f"No data available for {url_id}, {year}-{month}")
            return pd.DataFrame(columns=['dt', 'TMIN', 'TMAX', 'RAIN', 'WINDDIR', 'WINDSPEED'])
