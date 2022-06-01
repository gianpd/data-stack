import os
import sys
import pathlib
import json

from uuid import uuid4

from dataclasses import dataclass

from app.utils import get_datetime_suffix, EVENT_PATH, logger

from typing import List, Dict, Optional, Union

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
    def ingest_raw(self, event: Dict) -> None:
        """
        It allows to save the received event to the disk, as received by the server (without perform any preprocessing).

        --Parameters:
         - event: Dict, the event as received by the server.

        return -> None
        """
        log_index = "data-ingest-pipe>"
        try:
            suffix = get_datetime_suffix()
            fname = EVENT_PATH + f'/event_{suffix}_{uuid4().hex[:7]}.json'
            logger.debug(f'{log_index} trying to write a json to {fname} ...')
            with open(fname, 'x') as f:
                json.dump(event, f)
                logger.debug(f'{log_index} JSON saved to {fname} ...')
                return
        except:
            raise DataIngestException(f" Error in DataIngestPipeline: {sys.exc_info()[0]} {sys.exc_info()[1]}") 

    def event_check(self, event: Dict) -> Union[None, str]:
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