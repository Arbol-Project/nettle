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

# First value in variables should be dt
station_metadata_variables_schema = {
    "0": {
        'type': 'dict',
        'schema': {
            'column name': {
                'type': 'string',
                'required': True,
                'allowed': ['dt']
            }
        }
    }
}

# All values in variables should have column name, unit of measurement and na value
# All but the first should also have api name but unsure how to implement this
station_metadata_variables_valuesrules = {
    'type': 'dict',
    'schema': {
        'column name': {
            'type': 'string',
            'required': True
        },
        'unit of measurement': {
            'type': 'string',
            'required': True
        },
        'na value': {
            'type': 'string',
            'required': True
        }
    }
}

station_metadata_features_schema = {
    'type': 'dict',
    'schema': {
        'type': {
            'type': 'string',
            'required': True,
            'allowed': ['Feature']
        },
        'geometry': {
            'type': 'dict',
            'required': True
        },
        'properties': {
            'type': 'dict',
            'required': True,
            'schema': {
                'station name': {
                    'type': 'string',
                    'required': True
                },
                'date range': {
                    'type': 'list',
                    'required': True
                },
                'variables': {
                    'type': 'dict',
                    'required': True,
                    'schema': station_metadata_variables_schema,
                    'valuesrules': station_metadata_variables_valuesrules
                },
            }
        }
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
        'required': True,
        'schema': station_metadata_features_schema
    }
}

metadata_validator = Validator(metadata_schema, allow_unknown=True)
station_metadata_validator = Validator(
    station_metadata_schema, allow_unknown=True)
