from cerberus import Validator

metadata_schema = {
    'name': {
        'type': 'string',
        'required': True
    },
    'data source': {
        'type': 'string',
        'required': True
    },
    'contact': {
        'type': 'string',
        'required': True,
        'nullable': True
    },
    'compression': {
        'type': 'string',
        'required': True
    },
    'documentation': {
        'type': 'string',
        'required': True
    },
    'tags': {
        'type': 'list',
        'required': True
    },
    'time generated': {
        'type': 'string',
        'required': True
    },
    'previous hash': {
        'type': 'string',
        'required': True,
        'nullable': True
    },
    'data dictionary': {
        'type': 'dict',
        'required': True
    }
}

station_metadata_schema = {
    'type': {
        'type': 'string',
        'required': True,
        'allowed': ['FeatureCollection']
    },
    'features': {
        'type': 'list',
        # 'schema': {
        #     'ps': {'type': 'float', 'required': True},
        #     'pp': {'type': 'float', 'required': True},
        #     'ls': {'type': 'float', 'required': True},
        #     'lp': {'type': 'float', 'required': True},
        #     'ab': {'type': 'float', 'required': True}
        # },
    }
}

metadata_validator = Validator(metadata_schema, allow_unknown=True)
station_metadata_validator = Validator(station_metadata_schema, allow_unknown=True)
