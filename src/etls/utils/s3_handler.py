from ..conf import settings
import pandas as pd


class S3Handler:
    # for accessing historical s3 data
    AWS_ACCESS_KEY = settings.AWS_ACCESS_KEY
    AWS_SECRET_KEY = settings.AWS_SECRET_KEY
    S3_STATION_BUCKET = settings.S3_STATION_BUCKET

    @staticmethod
    def read_csv_from_station(path):
        path = f'{S3Handler.S3_STATION_BUCKET}/{path}'
        try:
            csv = pd.read_csv(path, storage_options={
                "key": S3Handler.AWS_ACCESS_KEY, "secret": S3Handler.AWS_SECRET_KEY})
            return csv
        except FileNotFoundError:
            # warning logged in StationSet get_historical_dataframe
            return None

    def add_to_s3(self):
        pass
