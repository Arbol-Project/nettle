bom_metadata = {
    "name": "BOM Australia Weather Station Data",
    "data source": "http://www.bom.gov.au/",
    "contact": "",
    "compression": None,
    "documentation": "Weather data from the Bureau of Meteorology in Australia covering roughly 500 stations",
    "tags": [
        "temperature",
        "rain",
        "wind",
        "australia"
    ],
    "time generated": "2023-08-30 12:24:31.882477",
    "previous hash": None,
    "data dictionary": {
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
    }
}

bom_stations_metadata = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [
                    "126.3867",
                    "-14.0900"
                ]
            },
            "properties": {
                "station name": "TRUSCOTT",
                "previous hash": "",
                "country": "",
                "file name": "TRUSCOTT.csv",
                "date range": [
                    "2022-08-01",
                    "2023-08-30"
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
                "code": "IDCJDW6141"
            }
        },
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
                    "2022-08-01",
                    "2023-08-30"
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

kalumburu_metadata = {
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