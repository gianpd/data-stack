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


logger.info('db-ingest> Preprocessing started ...')
events = list(map(lambda x: str(x), pathlib.Path('shared').glob('*.json')))
logger.info(f'Retrived {len(events)} events.')
output_path = 'events_preprocessing.csv'
preprocess_events(events, output_path)
logger.info('Reading preprocessed events df:')
df = pd.read_csv(output_path, index_col=0)
logger.info(df.head())
event = dict(df.iloc[0])
logger.info(f'events DB schema: {Events_pydantic.schema_json(indent=4)}')
event = Events_pydantic.parse_obj(event)
logger.info(f'Trying to upload event: {event} to Events Table')
logger.info(f'Events pydantic {event}')
run_async(run(event))