import logging
logging.basicConfig(filename='application.log', filemode='w', format='%(asctime)s - %(levelname)s --> %(message)s', level=logging.INFO)
from utils.Costants import levelLog


def writeLog(level, cl, message):
    if level == levelLog.INFO:
        logging.info("{ " + cl + " } " + message)
    elif level == levelLog.WARNING:
        logging.warning("{ " + cl + " } " + message)
    elif level == levelLog.DEBUG:
        logging.debug("{ " + cl + " } " + message)
    elif level == levelLog.ERROR:
        logging.error("{ " + cl + " } " + message)
    elif level == levelLog.CRITICAL:
        logging.critical("{ " + cl + " } " + message)