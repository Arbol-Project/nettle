from .station_set.date_handler import DateHandler
from managers.StationSet import StationSet
import datetime
import os
import pandas as pd
pd.options.mode.chained_assignment = "raise"


class CWV2(StationSet):
    BASE_URL = "http://mip-prd-web.azurewebsites.net/CustomDataDownload?LatestValue=true&Applicable=applicableAt&FromUtcDatetime={}T00:00:00.000Z&ToUtcDateTime={}T23:00:00.000Z&PublicationObjectStagingIds={}"
    BASE_METADATA = {
        "name": "Composite Weather Variable",
        "data source": BASE_URL,
        "compression": "None",
        "documentation": "National Grid (UK) data used as a proxy for natural gas demand. Further info can be found at https://www.nationalgrid.com/gas-transmission/document/132516/download",
        "station metadata": "stations.json",
        "tags": ["energy", "uk"]
    }

    def climate_measurement_span(self):
        return DateHandler.SPAN_DAILY

    @staticmethod
    def _concatenate_together_all_dfs(dfs):
        return pd.concat(dfs, ignore_index=True)

    @staticmethod
    def _parse_station_out_dataframe_data_item(df):
        # parse the station out of the Data Item column
        # example "Composite Weather Variable, Actual, LDZ(EM), D+1" becomes "EM"
        return [x[-8:-6] for x in df['Data Item']]

    @staticmethod
    def _format_date_dataframe(df):
        return [datetime.datetime.strptime(x, '%d/%m/%Y') for x in df["Applicable For (Gas Day)"]]

    @staticmethod
    def _convert_dates_to_actual_dates(station_df_dt):
        return [datetime.datetime.strptime(
            x, '%Y-%m-%d') for x in station_df_dt]

    def _calculate_180_days_jumps(self, new_start):
        # Can only pull 180 days at a time, this will give
        # us the number of days required from 2021-01-01
        # When our historical data stops
        diff = (self.today_with_time.date() - new_start).days
        jumps = [x * 180 for x in range(0, 1 + int(diff / 180))]
        if jumps[-1] != diff:
            jumps.append(diff)

        return jumps

    def _get_url_id_from_station_dict(self, station_id):
        return self.STATION_DICT[station_id]["code"]

    def _fetch_data_from_url(self, new_start, station_id):
        jumps = self._calculate_180_days_jumps(new_start)
        result = []
        url_id = self._get_url_id_from_station_dict(station_id)

        # Iterate through jumps and pull 180 days
        for i in range(len(jumps) - 1):
            temp_start = str(new_start + datetime.timedelta(days=jumps[i]))
            temp_end = str(new_start + datetime.timedelta(jumps[i + 1]))
            result.append(pd.read_html(self.BASE_URL.format(
                temp_start, temp_end, url_id))[0])

        return result

    def _fetch_today_data_from_url(self, station_id):
        url_id = self._get_url_id_from_station_dict(station_id)
        return [pd.read_html(self.BASE_URL.format(
            self.today_with_time.date(), self.today_with_time.date(), url_id))[0]]

    # maybe move try except inside to latest_hash
    def _get_latest_hash(self):
        try:
            return self.ipfs_handler.latest_hash()
        except KeyError:
            return None

    def get_new_start(self, station_id):
        if "date range" in self.STATION_DICT[station_id]:
            # previous end date + 1 days
            return self.STATION_DICT[station_id]["date range"][1].date() + datetime.timedelta(days=1)
        else:
            # the oldest date NOT IN our static history file
            return datetime.date(2021, 1, 1)

    def _get_proper_dataframes_based_on_date_range(self, station_id):
        if "date range" in self.STATION_DICT[station_id]:
            # empty list to collect new data in 180 days at a time
            return []
        else:
            # instead of an empty list we start the list with our historical data
            return [self.get_historical_dataframe(station_id=station_id)]

    # shouldn't this be in station_set?
    # shouldn't this be in write_metadata section?
    def _write_base_metadata_info(self):
        self.metadata = self.BASE_METADATA
        # Should this be self.today + time?
        self.metadata["time generated"] = str(self.today_with_time)
        self.metadata["previous hash"] = self._get_latest_hash()

    def _read_single_station_df_from_csv(self, station_id):
        return pd.read_csv(os.path.join(self.file_handler.local_input_path(
        ), self.FILE_NAME_FORMAT.format(station_id)))

    def _get_dataframes_transformed(self, dataframes):
        new_dataframe = self._concatenate_together_all_dfs(dataframes)
        new_dataframe['Data Item'] = self._parse_station_out_dataframe_data_item(new_dataframe)
        new_dataframe['dt'] = self._format_date_dataframe(new_dataframe)
        return new_dataframe

    def on_update_prepare_initial_data(self, initial_data, **kwargs):
        initial_data['stations_updated_counter'] = 0

    def on_update_extract(self, station_id, data, **kwargs):
        data['new_start'] = self.get_new_start(station_id)
        dataframes = self._get_proper_dataframes_based_on_date_range(station_id)
        dataframes += self._fetch_data_from_url(data['new_start'], station_id)

        # Only happens if new start == today
        if len(dataframes) == 0:
            dataframes = self._fetch_today_data_from_url(station_id)

        data['extract_result'] = dataframes

    def on_update_transform(self, station_id, data, **kwargs):
        dataframes = data['extract_result']
        data['transform_result'] = self._get_dataframes_transformed(dataframes)

    def on_update_load(self, station_id, data, **kwargs):
        new_dataframe = data['transform_result']

        # write the stations out individually
        output_df = new_dataframe.loc[new_dataframe["Data Item"] == station_id].copy()
        output_df.sort_values(by='dt', inplace=True, ignore_index=True)
        output_df[["dt", "Value"]].to_csv(os.path.join(self.file_handler.local_input_path(
        ), self.FILE_NAME_FORMAT.format(station_id)), index=False)

    def on_update_station_verify(self, station_id, data, **kwargs):
        data['stations_updated_counter'] += self.compare_download_date_range(
            new_start=data['new_start'], new_data_df=data['transform_result'], station_id=station_id)
        data['station_verify_result'] = data['stations_updated_counter']

    # Kiran, please verify if this is working properly
    def on_update_verify(self, data, **kwargs):
        return data['stations_updated_counter'] != len(data['stations_ids'])

    def on_parse_initial_data(self, data, **kwargs):
        pass

    # Maybe improve here loading all stations df in one pass
    def on_parse_extract(self, station_id, data, **kwargs):
        data['df'] = self._read_single_station_df_from_csv(station_id)

    def on_parse_transform(self, station_id, data, **kwargs):
        station_df = data['df']
        station_df["dt"] = self._convert_dates_to_actual_dates(station_df["dt"])
        # cut out any duplicate rows
        station_df.drop_duplicates(
            subset=['dt'], ignore_index=True, inplace=True)
        # rename Value column to something more useful
        station_df['CWV'] = station_df['Value']
        # there shouldn't be any other columns but cut down to date and value
        station_df = station_df[['dt', 'CWV']]

        data['df'] = station_df

    def on_parse_load(self, station_id, data, **kwargs):
        # Make sense to move this to some other place?
        # shouldn't this be in station_set?
        # shouldn't this be in write_metadata section?
        self._write_base_metadata_info()

    def on_parse_verify(self, **kwargs):
        return True

    def verify(self, **kwargs):
        return True
