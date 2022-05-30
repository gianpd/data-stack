import os
import sys
from datetime import datetime

import logging
logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                level=logging.DEBUG, datefmt=None)
logger = logging.getLogger("Data-Ingest")

#TODO: move all env to a config file.
EVENT_PATH = os.path.join('/usr/src/app', 'shared')

def get_datetime_suffix():
    n = datetime.now()
    return datetime.strftime(n, '%Y%m%d_%s')