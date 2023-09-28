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
        'required': True,
        'nullable': True
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


# function for ensuring API names are uppercase
def api_uppercase_check(field, value, error):
    if value.upper() != value:
        error(
            field, f"api name should be uppercase, try {value.upper()} instead of {value}")


# variable key "0" in your variables dict should follow this schema
variable_schema_0 = {
    'column name': {
        'type': 'string',
        'required': True,
        'allowed': ['dt']
    },
    'unit of measurement': {
        'type': 'string',
        'required': True
    },
    'na value': {
        'type': 'string',
        'required': True
    },
    'api name': {
        'required': False
    }
}


# all other variables should follow this schema
variable_schema_else = {
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
    },
    'api name': {
        'required': False,
        'check_with': api_uppercase_check
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
                    'valuesrules': {
                        'type': 'dict',
                        'oneof_schema': [variable_schema_0, variable_schema_else]
                    }
                }
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
