class FailedStationException(Exception):
    """Raised when the station in nettle fails"""
    pass


class MetadataInvalidException(Exception):
    """Raised when the metadata is invalid"""
    pass
