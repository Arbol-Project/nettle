class FailedStationException(Exception):
    """Raised when the station in nettle fails"""
    pass


class MetadataInvalidException(Exception):
    """Raised when the metadata is invalid"""
    pass


class DataframeInvalidException(Exception):
    """Raised when the dataframe is invalid"""
    pass
