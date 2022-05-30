"""
Data-Life-Cycle Manager Python class: A data manager class for managing all the data-ingest file stuff.
"""

import os
import sys
import json
import pathlib

from utils import *

import zipfile

from datetime import datetime

from dataclasses import dataclass

@dataclass
class DLCM:

    def apply_zip_events_job(self, start_day=None, end_day=None):
        """
        Allows to create a zip archive with the received json events. The zip archive can be created for the current day,
        or for a range of days (start_day, end_day)

        --Parameters
         - start_day: str in the format %Y%m%d without any space or delimiter
         - end_day: str in the format %Y%m%d without any space or delimiter

        """

        if start_day and end_day:
            current_date = start_day + '_' + end_day
        else:
            current_date = get_datetime_suffix().split('_')[0]

        dir_path = EVENT_PATH + '/' + current_date
        logger.info(f'*** Trying to create dir at {dir_path}')
        pathlib.Path(dir_path).mkdir(exist_ok=True)
        zip_fname = dir_path + '/events.zip'
        # check zip archive does not already exist:
        assert not pathlib.Path(zip_fname).is_dir(), f'{zip_fname} already exist.'
        events_json_paths = list(map(lambda x: str(x), pathlib.Path(EVENT_PATH).glob('*.json')))
        logger.info(f'*** Founded {len(events_json_paths)} to be zipped at {zip_fname}')
        self.___zip_day_events(zip_fname, events_json_paths)


    def ___zip_day_events(self, zip_fname: str, events_json_paths: str):
        log_prefix = 'DLCM_zip_day_events>'
        try:
            with zipfile.ZipFile(zip_fname, mode='x', compression=zipfile.ZIP_DEFLATED) as  my_zip:
                for event in events_json_paths:
                    my_zip.write(event)
                    os.remove(event)
            logger.info(f'{log_prefix} ZIP DAY EVENTS done.')
        except:
            raise DLCMException(f'Error in DLCM class: {sys.exc_info()[0]} {sys.exc_info()[1]}')

class DLCMException(Exception):
    pass


if __name__ == '__main__':
    dlcm = DLCM()
    dlcm.apply_zip_events_job()