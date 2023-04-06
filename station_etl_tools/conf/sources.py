# sources.py
#
# This file instatiates all climate data manager sets and groups them into lists that provide an easy way
# to access sources by organization.
#
# When using this file, it makes sense to import it and then access the classes using the `SOURCES` list or
# some subset of it. This way, classes are only instatiated once, and classes can be processed by iterating
# through the list.
#
# When using this file, it makes sense to import it and then access the classes using the `SOURCES` list or
# some subset of it. This way, classes are only instantiated once, and classes can be processed by iterating
# through the list.


import sys
import inspect
from ..managers.cwv2 import CWV2
from ..managers.bom2 import BOM2

SOURCES = [
    tup[1]
    for tup in inspect.getmembers(sys.modules[__name__], inspect.isclass)
]

# use the following to get a set manager object from a string:
#
#     $ get_set_manager_from_name("chirps_final_05")
#     $ -> <Class: CHIRPSFinal05>


# this replaces the passed string for each source with a set manager instance
def get_set_manager_from_name(name):
    for source in SOURCES:
        if source.name() == name:
            return source
    print(f"failed to set manager from name {name}")


def get_set_manager_from_key(key):
    for source in SOURCES:
        if source.json_key() == key:
            return source
    print(f"failed to get a manager from key {key}")
