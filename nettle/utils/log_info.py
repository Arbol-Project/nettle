import logging

class LogInfo:
    def __init__(self, log, class_name):
        self._log = log
        self.class_name = class_name

    def set_log(self, log):
        self._log = log

    def log(self, level, message):
        '''
        Add formatting or any processing before sending to class' internal log function
        '''
        self._log(level, f"[{self.class_name}] {message}")

    def info(self, message):
        '''
        Log a message at INFO level
        '''
        if self._log == print:
            self.log("\033[90mINFO    \033[0m", message)
        else:
            self.log(logging.INFO, message)

    def debug(self, message):
        '''
        Log a message at DEBUG level
        '''
        if self._log == print:
            self.log("\033[90mDEBUG    \033[0m", message)
        else:
            self.log(logging.DEBUG, message)

    def warn(self, message):
        '''
        Log a message at WARN level
        '''
        if self._log == print:
            self.log("\033[93mWARN    \033[0m", message)
        else:
            self.log(logging.WARN, message)

    def error(self, message):
        '''
        Log a message at ERROR level
        '''
        if self._log == print:
            self.log("\033[91mERROR    \033[0m", message)
        else:
            self.log(logging.ERROR, message)
