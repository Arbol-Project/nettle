from pandera import Column
from pandera import DataFrameSchema
from pandera.errors import SchemaErrors as DataframeValidationErrors
from pandera import Check
from pandera.engines.pandas_engine import Date


schema = DataFrameSchema(
    {
        # required=True by default
        # nullable=False by default
        # "dt": Column(Date, checks=dt_checks),
        "dt": Column(Date(to_datetime_kwargs={"format": "%Y-%m-%d"}), coerce=True),
        "PRCP": Column(float, nullable=True, required=False),
        "TMIN": Column(float, nullable=True, required=False),
        "TMAX": Column(float, nullable=True, required=False),
        "TAVG": Column(float, nullable=True, required=False),
        "WINDDIR": Column(str, nullable=True, required=False),
        "WINDSPEED": Column(float, nullable=True, required=False),
        "RAIN": Column(float, nullable=True, required=False),
        "SNOW": Column(float, nullable=True, required=False),
        "SNWD": Column(float, nullable=True, required=False),
        "WSF5": Column(float, nullable=True, required=False),
        "WESD": Column(float, nullable=True, required=False),
        "SEA_SURFACE_TEMPERATURE_WMEAN": Column(float, nullable=True, required=False),
        "CLIMATOLOGY": Column(float, nullable=True, required=False),
        "ANOMALIES": Column(float, nullable=True, required=False),
        "SALINITY_WMEAN": Column(float, nullable=True, required=False),
    }
)

# dt_checks = [
#     # a vectorized check that returns a boolean series
#     Check(lambda s: s > 0, element_wise=False),
#
#     # an element-wise check that returns a bool
#     Check(lambda x: x > 0, element_wise=True)
# ]

dataframe_validator = schema
