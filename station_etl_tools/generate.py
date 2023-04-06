# import json
import argparse
# from importlib.resources import open_text
from .conf.logging import initialize_logging
# from .conf.sources import *
# from .station_set import StationSet

LOG_PATH = "/var/log/arbol/parsing-info.log"
DEBUG_LOG_PATH = "/var/log/arbol/parsing-debug.log"
CONF_FILE = 'etls/conf/generate.json'


def update(logging, source_instance):
    logging.info("updating local input")
    # update_local_input will return True if parse should be triggered
    return source_instance.update_local_input()


def parse(logging, source_instance, trigger_parse):
    if trigger_parse:
        logging.info("parsing {}".format(source_instance))
        # parse will return `True` if new data was parsed
        if source_instance.parse():
            logging.info("performing verification on {}".format(source_instance))
            return True
        else:
            logging.info("no new data parsed, ending here")
    else:
        logging.info("no new data detected and parse not set to force, ending here")

    return False


def verify(logging, perform_validation):
    if perform_validation:
        pass


def get_set_manager_from_name(name):
    pass
    # from .station_set import StationSet
    # print([cls.__name__ for cls in StationSet.__subclasses__()])
    #
    # sub_classes = StationSet.__subclasses__()
    # sub_classes_names = [sub_class.name() for sub_class in sub_classes]
    #
    # print('opaaa')
    # print(sub_classes_names)
    # print(sub_classes)
    #
    # if name is None:
    #     print("\nNo source specified in command. Use one of the following, or run './generate.py -h' for help.\n")
    #     print(*sub_classes_names)
    #     print()
    #     exit()
    #
    # if name not in sub_classes_names:
    #     raise ValueError(f"{name} not a valid source key")
    #
    # for source in sub_classes:
    #     if source.name() == name:
    #         return source
    # print(f"failed to set manager from name {name}")


def generate_result(source_class, **kwargs):
    config = initialize_configuration(kwargs)
    logging = initialize_logging(LOG_PATH, DEBUG_LOG_PATH, config['verbose'])
    source_instance = source_class(
        log=logging.log, custom_output_path=config['custom_output_path'],
        custom_metadata_head_path=config['custom_head_metadata'],
        custom_latest_hash=config['custom_latest_hash'], publish_to_ipns=config['publish_to_ipns'],
        rebuild=config['rebuild'], force_http=config['force_http']
    )
    logging.info("processing {}".format(source_instance))
    trigger_parse = update(logging, source_instance)
    perform_validation = parse(logging, source_instance, trigger_parse)
    verify(logging, perform_validation)


def initialize_configuration(kwargs):
    return {
        "verbose": kwargs.get('verbose') or False,
        "custom_output_path": kwargs.get('custom_output_path'),
        "custom_head_metadata": kwargs.get('custom_head_metadata'),
        "custom_latest_hash": kwargs.get('custom_latest_hash'),
        "publish_to_ipns": kwargs.get('publish_to_ipns'),
        "rebuild": kwargs.get('rebuild') or False,
        "force_http": kwargs.get('force_http') or False,
        "suppress_add": kwargs.get('suppress_add') or False
    }
    # # Deprecated since version 3.11
    # with open_text("etls.conf", "generate.json") as generate_json:
    # # use this if update python version
    # # with files("etls").joinpath("conf/generate.json").open('r', encoding='utf-8') as generate_json:
    #     # parsed_json = json.load(generate)
    #     return json.load(generate_json)


# def print_valid_source_arguments(valid_source_keys):
#     for ii, source in enumerate(sorted(valid_source_keys)):
#         print(source.ljust(50), end="")
#         if not ii % 3:
#             print()


def parse_command_line(command=None):
    # valid_source_keys = [s.name() for s in SOURCES]
    parser = argparse.ArgumentParser(
        description='''This utility parses climate data into Arbol's gzipped CSV format, using a merkledag node on IPFS to store
                the parsed data, and then verifies the parsed data before completing. It can retrieve original published data by 
                checking remote locations for updates and can be set to run at a specified interval as a daemon.''',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # parser.add_argument("source", nargs="?", default=None, help="any of {}".format(valid_source_keys))
    parser.add_argument("source", nargs="?", default=None)
    arguments = parser.parse_args(command)
    source_class = get_set_manager_from_name(arguments.source)

    return source_class


def main(command=None):
    source_class = parse_command_line(command)
    generate_result(source_class)


if __name__ == "__main__":
    main()
