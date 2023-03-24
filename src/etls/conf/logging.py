import logging
import logging.handlers


def initialize_logging(log_path, debug_log_path, verbose=False):
    '''
    Initialize the logging module to write to a file and stdout. Use WatchedFileHandler to avoid conflicting
    with logrotate.
    '''
    LOG_PATH = log_path
    DEBUG_LOG_PATH = debug_log_path
    # use verbose flag to determine what level to print to stdout
    if verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    # basic logging handles printing to stdout
    logging.basicConfig(level=level, format="%(levelname)-8s %(message)s")
    logger = logging.getLogger('')
    # add a handler for writing only info level statements to a file even if verbose mode is on
    info_log_handler = logging.handlers.WatchedFileHandler(LOG_PATH)
    info_log_handler.setLevel(logging.INFO)
    system_log_formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", "%m/%d/%Y %I:%M %p")
    info_log_handler.setFormatter(system_log_formatter)
    logger.addHandler(info_log_handler)
    # add a separate handler for writing both info and debug level statements to a file that will only be active in verbose mode
    if level == logging.DEBUG:
        debug_log_handler = logging.handlers.WatchedFileHandler(DEBUG_LOG_PATH)
        debug_log_handler.setLevel(logging.DEBUG)
        debug_log_handler.setFormatter(system_log_formatter)
        logger.addHandler(debug_log_handler)

    return logging