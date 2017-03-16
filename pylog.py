__author__ = 'tws626'
import logging

class Pylog():
    def __init__(self):
        pass

    #########################################################
    #Method: Generic logger class
    #Function: Debugging and logging
    #########################################################

    def log_formatter(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler('debug.log')
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger