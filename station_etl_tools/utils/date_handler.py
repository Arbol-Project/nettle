import datetime
import re


class DateHandler:
    DATE_FORMAT_FOLDER = "%Y%m%d"
    DATE_HOURLY_FORMAT_FOLDER = "%Y%m%d%H"
    DATE_FORMAT_METADATA = "%Y/%m/%d"

    SPAN_HOURLY = "hourly"
    SPAN_DAILY = "daily"
    SPAN_WEEKLY = "weekly"
    SPAN_MONTHLY = "monthly"
    SPAN_YEARLY = "yearly"
    SPAN_SEASONAL = "seasonal"

    @staticmethod
    def convert_date_range(date_range):
        '''
        Convert a JSON text/isoformat date range into a python datetime object range
        '''
        if re.match(".+/.+/.+", date_range[0]):
            start, end = [datetime.datetime.strptime(
                d, DateHandler.DATE_FORMAT_METADATA) for d in date_range]
        else:
            start, end = [datetime.datetime.fromisoformat(
                d) for d in date_range]
        return start, end
