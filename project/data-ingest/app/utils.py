import os
import sys
from datetime import datetime, timedelta

from functools import lru_cache, wraps

import logging
logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                level=logging.DEBUG, datefmt=None)
logger = logging.getLogger("Data-Ingest")

#TODO: move all env to a config file.
EVENT_PATH = os.path.join('/usr/src/app', 'shared')
MUST_HAVE_FIELDS = [
    "client.user_id", # the ID of the user that did the upload or the download
    "direction", 
    "timestamp", # when the event was sent to the server
    "size", # the total size of the uploaded/downloaded payload, in bytes
    "time.backend", # the time needed to transfer the payload, in milliseconds
    "status", # the operation’s result - [success, fail]
]


def get_datetime_suffix():
    n = datetime.now()
    return datetime.strftime(n, '%Y%m%d_%s')

def timed_lru_cache(seconds: int, maxsize: int =128):
    """
    Decorator function: allows to save on a LRU cache the received events till the expire time is over.

    The decorated function will save the cache events to the disk IF the expire time is over.
    """
    def wrapper_cache(func):
        func = lru_cache(maxsize=maxsize)(func)
        func.lifetime = timedelta(seconds=seconds)
        func.expiration = datetime.utcnow() + func.lifetime
        @wraps(func)
        def wrapped_func(*args, **kwargs):
            if datetime.utcnow() >= func.expiration:
                func.cache_clear()
                func.expiration = datetime.utcnow() + func.lifetime
                return func(*args, **kwargs) # save events to disk
            return {"Response": "in cache"} # no expire time: keep events on cache
        return wrapped_func
    return wrapper_cache