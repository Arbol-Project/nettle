import datetime
import os
import pandas as pd
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from dateutil.rrule import rrule, MONTHLY
from station_etl_tools.station_set import StationSet


class BOM(StationSet):
    """
    A class for weather station data from the Australian Bureau of Meteorology (BOM)
    """
    BASE_URL = "http://www.bom.gov.au/"
    FILE_NAME_FORMAT = "{0}.csv"
    BASE_METADATA = {
        "name": "BOM Australia Weather Station Data",
        "data source": BASE_URL,
        "compression": "None",
        "documentation": "Weather data from the Bureau of Meteorology in Australia covering roughly 500 stations",
        "station metadata": "stations.json",
        "tags": ["temperature", "rain", "wind", "australia"]
    }

    def climate_measurement_span(self):
        return self.date_handler.SPAN_DAILY

    def single_df(self, month, year, url_id):
        """
        Given a month, year and ID, return all the
        climate data for that month. Will return an empty dataframe
        with columns ['Date','Tmin','Tmax','Rain','Winddir','Windspeed']
        if anything goes wrong
        """
        try:
            new_url = self.BASE_URL + \
                f'climate/dwo/{year+month}/text/{url_id}.{year+month}.csv'
            req = Request(new_url)
            req.add_header(
                'User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0')
            content = str(urlopen(req).read())

            raw_rows = content.split('\\r\\n')
            rows = []
            # There is some intro guff until empty line ''
            # Start reading from after this line onwards
            for i in range(raw_rows.index('')+1, len(raw_rows)):
                # Ignore first character it's always a space
                row = raw_rows[i][1:].split(',')
                # Replace existing apostrophes/speech marks
                row = [x.replace("'", "").replace('"', '') for x in row]
                rows.append(row)

            df = pd.DataFrame(columns=rows[0], data=rows[1:])

            cols = ['Date']
            # Columns contain unreferenceable character for degrees c
            # iterate through and get column names from dataframe instead
            for column in df.columns:
                for key in ['maximum', 'minimum', 'rainfall']:
                    if key in column.lower():
                        cols.append(column)

            # Last column is time of max wind gust, remove
            cols = cols[:-1]
            df = df[cols]
            # Rename to avoid confusing unreferenceable character situation
            df.columns = ['dt', 'TMIN', 'TMAX',
                          'RAIN', 'WINDDIR', 'WINDSPEED']
            return df

        except HTTPError:
            self.log.info(f"No data available for {url_id}, {year}-{month}")
            return pd.DataFrame(columns=['dt', 'TMIN', 'TMAX', 'RAIN', 'WINDDIR', 'WINDSPEED'])

    def _get_dfs_with_formatted_data(self, new_start, station_id, url_id):
        dfs = []
        # Need to replace day with 1st of month to avoid skipping months with rrule
        for dt_month in rrule(MONTHLY, dtstart=new_start.replace(day=1), until=self.today_with_time.replace(day=1)):

            # date vars
            year = str(dt_month.year)
            month = str(f"{dt_month.month:02}")

            dfs.append(self.single_df(month, year, url_id))

        station_df = pd.concat(dfs, ignore_index=True)

        # index
        station_df['dt'] = [datetime.datetime.strptime(
            x, '%Y-%m-%d') for x in station_df['dt']]
        station_df.sort_values(by='dt', inplace=True, ignore_index=True)

        return station_df

    def verify(self, **kwargs):
        return True

    # NEW STUFF

    def get_new_start(self, station_id):
        if "date range" in self.STATION_DICT[station_id]:
            # previous end date + 1 days
            return self.STATION_DICT[station_id]["date range"][1].date() + datetime.timedelta(days=1)
        else:
            # Data available up to 14 months back
            new_start = self.today_with_time.date() + datetime.timedelta(weeks=-55)
            # hard code day to the 1st of the month
            return new_start.replace(day=1)

    # move this to station set?
    def _get_latest_hash(self):
        try:
            return self.store.latest_hash()
        except KeyError:
            return None

    # move this to station set?
    def _write_base_metadata_info(self):
        self.metadata = self.BASE_METADATA
        # Should this be self.today + time?
        self.metadata["time generated"] = str(self.today_with_time)
        self.metadata["previous hash"] = self._get_latest_hash()

    def _read_single_station_df_from_csv(self, station_id):
        return pd.read_csv(os.path.join(self.file_handler.local_input_path(
        ), self.FILE_NAME_FORMAT.format(station_id)))

    def on_update_prepare_initial_data(self, initial_data, **kwargs):
        initial_data['stations_updated_counter'] = 0

    def on_update_extract(self, station_id, data, **kwargs):
        data['new_start'] = self.get_new_start(station_id)

    def on_update_transform(self, station_id, data, **kwargs):
        data['transform_result'] = self._get_dfs_with_formatted_data(
            new_start=data['new_start'], station_id=station_id,
            url_id=self.STATION_DICT[station_id]["code"])

    def on_update_load(self, station_id, data, **kwargs):
        dataframes = data['transform_result']
        dataframes.to_csv(os.path.join(self.file_handler.local_input_path(
        ), self.FILE_NAME_FORMAT.format(station_id)), index=False)

    def on_update_station_verify(self, station_id, data, **kwargs):
        data['stations_updated_counter'] += self.compare_download_date_range(
            new_start=data['new_start'], new_data_df=data['transform_result'], station_id=station_id)
        data['station_verify_result'] = data['stations_updated_counter']

    def on_update_verify(self, data, **kwargs):
        return data['stations_updated_counter'] != len(data['stations_ids'])

    def on_parse_initial_data(self, data, **kwargs):
        self._write_base_metadata_info()

    def on_parse_extract(self, station_id, data, **kwargs):
        """
        Outputs separate csv for each station
        formats and writes all metadata
        """
        data['df'] = self._read_single_station_df_from_csv(station_id)

    # very similar to cwv2
    def on_parse_transform(self, station_id, data, **kwargs):
        station_df = data['df']
        # convert dates to actual dates
        station_df["dt"] = [datetime.datetime.strptime(
            x, '%Y-%m-%d') for x in station_df["dt"]]
        # cut out any duplicate rows
        station_df.drop_duplicates(
            subset=['dt'], ignore_index=True, inplace=True)
        data['df'] = station_df

    def on_parse_load(self, station_id, data, **kwargs):
        pass

    def on_parse_verify(self, **kwargs):
        return True
