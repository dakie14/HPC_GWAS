import logging
import time

class ProcessLogger:

    def __init__(self, name):
        self.name = name

    def __format_logger(self, logger, msg_prefix='%(message)s', level=logging.INFO):
        logger.setLevel(level)

        if not logger.handlers:
            console = logging.StreamHandler()
            console.setLevel(logging.INFO)
            formatter = logging.Formatter(msg_prefix)
            console.setFormatter(formatter)
            logger.addHandler(console)

        return logger

    def get_time(self):
        t = time.localtime()
        return str(t.tm_mday) + "/" + str(t.tm_mon) + "/" + str(t.tm_year) + " - " + str(t.tm_hour) + ":" + str(t.tm_min) + ":" + str(t.tm_sec)

    def get_process_logger(self, name):
        logger = logging.getLogger(name)
        return self.__format_logger(logger, msg_prefix=self.get_time() + " - " + name + ': %(message)s')

    def info(self, msg):
        logger = logging.getLogger(self.name)
        logger = self.__format_logger(logger, msg_prefix=self.get_time() + " - " + self.name + ': %(message)s')
        logger.info(msg)