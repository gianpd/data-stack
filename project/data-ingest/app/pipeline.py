import os
import sys
import pathlib
import json
import logging
import zipfile

from requests import get
logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                level=logging.DEBUG, datefmt=None)
logger = logging.getLogger("data-ingestion-pipeline")

from dataclasses import dataclass

from app.utils import get_datetime_suffix, EVENT_PATH


from typing import List, Dict, Optional

MUST_HAVE_FIELDS = [
    "client.user_id", # the ID of the user that did the upload or the download
    "direction", 
    "timestamp", # when the event was sent to the server
    "size", # the total size of the uploaded/downloaded payload, in bytes
    "time.backend", # the time needed to transfer the payload, in milliseconds
    "status", # the operation’s result - [success, fail]
]


@dataclass
class DataIngestPipeline:
    async def ingest_raw(self, event: Dict) -> None:
        """
        It allows to save the received event to the disk, as received by the server (without perform any preprocessing).

        --Parameters:
         - event: Dict, the event as received by the server.

        return -> None
        """
        try:
            suffix = get_datetime_suffix()
            fname = EVENT_PATH + f'/event_{suffix}.json'
            logger.debug(f'trying to write a json to {fname} ...')
            with open(fname, 'x') as f:
                json.dump(event, f)
                return
        except:
            raise DataIngestException(f" Error in DataIngestPipeline: {sys.exc_info()[0]} {sys.exc_info()[1]}") 

    def event_check(self, event: Dict) -> bool:
        """
        Event checker: checks if some crucial fields are present in the received event.

        --Parameters:
         - event: Dict, the event as received by the server.

        return -> Error: str (i.e the not-present field) / SUCCESS: None
        """
        keys = list(event.keys())
        for field in MUST_HAVE_FIELDS:
            if field not in keys:
                logger.error(field, 'not in keys')
                return field
        logger.info('Data ingest> event check passed.')
        return None
        

class DataIngestException(Exception):
    pass