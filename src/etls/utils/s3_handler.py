from ..conf import settings
import pandas as pd


class S3Handler:
    # for accessing historical s3 data
    AWS_ACCESS_KEY = settings.AWS_ACCESS_KEY
    AWS_SECRET_KEY = settings.AWS_SECRET_KEY
    S3_STATION_BUCKET = settings.S3_STATION_BUCKET

    def __init__(self, log):
        self.log = log
        if S3Handler.AWS_ACCESS_KEY is None or S3Handler.AWS_SECRET_KEY is None:
            self.log.info("AWS keys not setted. Can't connect to S3")

    @staticmethod
    def read_csv_from_station(path):
        if S3Handler.AWS_ACCESS_KEY is None or S3Handler.AWS_SECRET_KEY is None:
            return

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
