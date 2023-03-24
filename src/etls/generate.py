import json
import argparse
from .conf.logging import initialize_logging
from .conf.sources import *

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


def generate(source_class):
    config = initiliaze_configuration()
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


def initiliaze_configuration():
    with open(CONF_FILE) as generate_json:
        # parsed_json = json.load(generate)
        return json.load(generate_json)


def parse_command_line(command=None):
    valid_source_keys = [s.name() for s in SOURCES]
    parser = argparse.ArgumentParser(
        description='''This utility parses climate data into Arbol's gzipped CSV format, using a merkledag node on IPFS to store
                the parsed data, and then verifies the parsed data before completing. It can retrieve original published data by 
                checking remote locations for updates and can be set to run at a specified interval as a daemon.''',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("source", nargs="?", default=None, help="any of {}".format(valid_source_keys))
    arguments = parser.parse_args(command)

    if arguments.source is None:
        print("\nNo source specified in command. Use one of the following, or run './generate.py -h' for help.\n")
        print()
        exit()

    if arguments.source not in valid_source_keys:
        raise ValueError(f"{arguments.source} not a valid source key")

    source_class = get_set_manager_from_name(arguments.source)
    return source_class


def main(command=None):
    source_class = parse_command_line(command)
    generate(source_class)


if __name__ == "__main__":
    main()
