import os
from datetime import datetime

#TODO: move all env to a config file.
EVENT_PATH = os.path.join('/usr/src/app', 'shared')

def get_datetime_suffix():
    n = datetime.now()
    return datetime.strftime(n, '%Y%m%d_%s')