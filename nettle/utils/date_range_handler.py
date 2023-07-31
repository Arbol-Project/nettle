import datetime
import pandas as pd

class DateRangeHandler:
    DATE_FORMAT_FOLDER = "%Y%m%d"
    DATE_HOURLY_FORMAT_FOLDER = "%Y%m%d%H"
    DATE_FORMAT_METADATA = "%Y/%m/%d"

    SPAN_HOURLY = "hourly"
    SPAN_DAILY = "daily"
    SPAN_WEEKLY = "weekly"
    SPAN_MONTHLY = "monthly"
    SPAN_YEARLY = "yearly"
    SPAN_SEASONAL = "seasonal"

    # @staticmethod
    # def convert_date_range(date_range):
    #     '''
    #     Convert a JSON text/isoformat date range into a python datetime object range
    #     '''
    #     if re.match(".+/.+/.+", date_range[0]):
    #         start, end = [datetime.datetime.strptime(
    #             d, DateHandler.DATE_FORMAT_METADATA) for d in date_range]
    #     else:
    #         start, end = [datetime.datetime.fromisoformat(
    #             d) for d in date_range]
    #     return start, end

    @staticmethod
    def convert_date_range_str_to_date(begin_date: str, end_date: str) -> tuple[datetime.date, datetime.date]:
        return datetime.datetime.strptime(begin_date, '%Y-%m-%d').date(), \
            datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

    @staticmethod
    def convert_date_range_date_to_str(begin_date: datetime.date, end_date:datetime.date) -> tuple[str, str]:
        return begin_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

    @staticmethod
    def get_date_range_from_dataframe(
            dataframe: pd.DataFrame
    ) -> tuple[datetime.date, datetime.date]:
        return DateRangeHandler.convert_date_range_str_to_date(min(dataframe['dt']), max(dataframe['dt']))

    @staticmethod
    def get_date_range_from_metadata(
            station_metadata: dict
    ) -> tuple[datetime.date, datetime.date] | tuple[None, None]:
        if station_metadata['features'][0]['properties']['date range']:
            date_begin_str = station_metadata['features'][0]['properties']['date range'][0]
            date_end_str = station_metadata['features'][0]['properties']['date range'][1]

            if date_begin_str and date_end_str:
                return DateRangeHandler.convert_date_range_str_to_date(date_begin_str, date_end_str)
        return None, None

    def get_lowest_and_highest_date_range(
            self,
            dataframe: pd.DataFrame,
            station_metadata: dict
    ) -> tuple[str, str]:
        dataframe_date_begin, dataframe_end_date = self.get_date_range_from_dataframe(dataframe)
        metadata_date_begin, metadata_date_end = self.get_date_range_from_metadata(station_metadata)
        begin_date = min(dataframe_date_begin, metadata_date_begin) if metadata_date_begin else dataframe_date_begin
        end_date = max(dataframe_end_date, metadata_date_end) if metadata_date_end else dataframe_end_date
        return DateRangeHandler.convert_date_range_date_to_str(begin_date, end_date)