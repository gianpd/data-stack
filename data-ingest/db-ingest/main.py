import os
import sys
import pathlib

import time

import pandas as pd

from tortoise import run_async

from db import run_post
from preprocessing import *
from models.tortoise import Events_pydantic

import logging
logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                level=logging.INFO, datefmt=None)
logger = logging.getLogger("db-ingest")

import argparse

def main(events_dir: str, output_csv_path: str, users=False) -> None:
    """
    db-ingest main python processing: 
    1. Preprocess the recorded events -> output: new preprocessing events csv
    2. Upload the preprocessed events to the DB Events Table. 

    ---Parameters
     - events_dir: str (the recorded events directory path (the shared volume between microservices))
     - output_csv_path: str (the csv path of the preprocessed events)
     - users: bool (if uploading a Users or a Events db row)

     return None
    """
    logger.info('db-ingest> Preprocessing started ...')
    try:
        logger.info(f'events DB schema: {Events_pydantic.schema_json(indent=4)}')
        while True:
            events = list(map(lambda x: str(x), pathlib.Path(events_dir).glob('*.json')))
            logger.info(f'Retrived {len(events)} events.')
            if len(events):
                events_df = preprocess_events(events, output_csv_path)
                logger.info(events_df.head())
                logger.info('Creating db events ...')
                post_events = create_db_events(events_df)
                logger.info('Running async DB POST ...')
                run_async(run_post(post_events, users=users))
                logger.info('Removing json events ...')        
                for path in events:
                    #TODO: all processed events could be zipped instead to be removed.
                    _ = pathlib.Path(path).unlink()
                logger.info('json events removed') 
            time.sleep(10)
    except FileNotFoundError as e:
        raise ValueError(e)
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DB-INGEST main python process.')
    parser.add_argument('--events_dir', required=True, type=str, help='shared volume directory for getting json events')
    parser.add_argument('--output_csv_path', required=False, type=str, default='events_preprocessing.csv')
    parser.add_argument('--users', action='store_true')

    args = parser.parse_args()

    main(args.events_dir, args.output_csv_path, users=args.users)


