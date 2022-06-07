import os
import sys
import pathlib

import pandas as pd

from tortoise import run_async

from db import run
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
        events = list(map(lambda x: str(x), pathlib.Path(events_dir).glob('*.json')))
        logger.info(f'Retrived {len(events)} events.')
        preprocess_events(events, output_csv_path)
    except FileNotFoundError as e:
        raise ValueError(e)
        
    logger.info('Reading preprocessed events df:')
    df = pd.read_csv(output_csv_path, index_col=0)
    logger.info(df.head())
    event = dict(df.iloc[0])
    logger.info(f'events DB schema: {Events_pydantic.schema_json(indent=4)}')
    event = Events_pydantic.parse_obj(event) # Tortoise ORM pydandic event obj
    logger.info(f'Trying to upload event: {event} to Events Table')
    logger.info(f'Events pydantic {event}')
    run_async(run(event, users=users))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DB-INGEST main python process.')
    parser.add_argument('--events_dir', required=True, type=str, help='shared volume directory for getting json events')
    parser.add_argument('--output_csv_path', required=False, type=str, default='events_preprocessing.csv')
    parser.add_argument('--users', action='store_true')

    args = parser.parse_args()

    main(args.events_dir, args.output_csv_path, users=args.users)


