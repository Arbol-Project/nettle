from unittest import TestCase
from nettle.utils.date_range_handler import DateRangeHandler
from datetime import datetime
import pandas as pd


class DateRangeHandlerTestCase(TestCase):
    def setUp(self):
        self.date_str_1 = '2023-08-26'
        self.date_str_2 = '2023-08-29'
        self.date_1 = datetime(2023, 8, 26)
        self.date_2 = datetime(2023, 8, 29)
        self.df = pd.DataFrame(
            pd.date_range(start=self.date_str_1, end=self.date_str_2),
            columns=['dt'],
            dtype="string"
        )
        self.kalumburu_metadata = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            "126.6453",
                            "-14.2964"
                        ]
                    },
                    "properties": {
                        "station name": "KALUMBURU",
                        "previous hash": "",
                        "country": "",
                        "file name": "KALUMBURU.csv",
                        "date range": [
                            "2023-08-26",
                            "2023-08-29"
                        ],
                        "variables": {
                            "0": {
                                "column name": "dt",
                                "plain text description": "Date at which measurement is taken",
                                "unit of measurement": "YYYY-MM-DD",
                                "precision": "day",
                                "precision digits": "NA",
                                "na value": "NA"
                            },
                            "1": {
                                "column name": "TMIN",
                                "api name": "TMIN",
                                "plain text description": "Minimum temperature in the 24 hours from 9am. Sometimes only known to the nearest whole degree.",
                                "unit of measurement": "degC",
                                "precision": "0.1",
                                "precision digits": "1",
                                "na value": ""
                            },
                            "2": {
                                "column name": "TMAX",
                                "api name": "TMAX",
                                "plain text description": "Maximum temperature in the 24 hours from 9am. Sometimes only known to the nearest whole degree.",
                                "unit of measurement": "degC",
                                "precision": "0.1",
                                "precision digits": "1",
                                "na value": ""
                            },
                            "3": {
                                "column name": "WINDDIR",
                                "api name": "WINDDIR",
                                "plain text description": "Direction of strongest gust in the 24 hours to midnight",
                                "unit of measurement": "compass_points",
                                "precision": "NA",
                                "precision digits": "NA",
                                "na value": ""
                            },
                            "4": {
                                "column name": "WINDSPEED",
                                "api name": "WINDSPD",
                                "plain text description": "Speed of strongest wind gust in the 24 hours to midnight",
                                "unit of measurement": "km/h",
                                "precision": "1",
                                "precision digits": "1",
                                "na value": ""
                            },
                            "5": {
                                "column name": "RAIN",
                                "api name": "PRCP",
                                "plain text description": "Precipitation (rainfall) in the 24 hours to 9am. Sometimes only known to the nearest whole millimetre.",
                                "unit of measurement": "mm",
                                "precision": "0.1",
                                "precision digits": "1",
                                "na value": ""
                            }
                        },
                        "code": "IDCJDW6062"
                    }
                }
            ]
        }

    def test_convert_date_range_str_to_date(self):
        """Convert string date range to date"""
        self.assertEqual(
            DateRangeHandler.convert_date_range_str_to_date(self.date_str_1, self.date_str_2),
            (self.date_1.date(), self.date_2.date())
        )

    def test_convert_date_range_date_to_str(self):
        """Convert date to string date range"""
        self.assertEqual(
            DateRangeHandler.convert_date_range_date_to_str(self.date_1, self.date_2),
            (self.date_str_1, self.date_str_2)
        )

    def test_get_date_range_from_dataframe(self):
        self.assertEqual(
            DateRangeHandler.get_date_range_from_dataframe(self.df),
            (self.date_1.date(), self.date_2.date())
        )

    def test_get_date_range_from_metadata(self):
        self.assertEqual(
            DateRangeHandler.get_date_range_from_metadata(self.kalumburu_metadata),
            (self.date_1.date(), self.date_2.date())
        )

    def test_get_lowest_and_highest_date_range(self):
        self.assertEqual(
            DateRangeHandler.get_lowest_and_highest_date_range(self.df, self.kalumburu_metadata),
            (self.date_str_1, self.date_str_2)
        )