import os
import sys
import pathlib
import json

import pandas as pd
pd.set_option('display.max_columns', None)

import logging
logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                level=logging.INFO, datefmt=None)
logger = logging.getLogger("db-ingest")

TO_KEEP = ['id', 'direction', 'size', 'status', 'time.backend', 'timestamp']

def get_event_records(event_paths: list) -> list:
    dd = []
    for i, event_path in enumerate(event_paths):
        with open(event_path) as f:
            dd.append(json.load(f))
            logger.info(f'Retrived event {event_paths[i]}: {dd[i]}')
    return dd

def preprocess_events(events: list[str], output_path: str) -> None:
    """
    Load the recorded events and create a dataframe with the wanted columns, such that the Table Events DB SCHEMA.

    --Parameters
     - events: list of str (paths of the events)
     - output_path: str (output path with extension .csv)
    """
    if len(events) == 0:
        raise ValueError('No events path')

    p = pathlib.Path(output_path)
    logger.info(f'Output csv path: {output_path}')
    output_path = output_path if p.suffix == 'csv' else p.stem + '.csv'
    logger.info(f'Output csv path: {output_path}')

    dd = get_event_records(events)
    logger.info(f'Generating events df starting from {len(dd)} records...')
    events_df = pd.DataFrame.from_records(dd)
    logger.info(f'Event DataFrame created with len: {len(events_df)}')
    logger.info(f'Event DataFrame: reducing columns to - {TO_KEEP}')
    events_df = events_df.loc[:, TO_KEEP]
    events_df = events_df.rename(columns={'id': 'user_id'})
    logger.info(events_df.head(2))
    ### TRANSFER SPEED && TRANSFER TIME
    logger.info(f'Adding new columns to events ...')
    K = 1e3
    events_df['size'] = events_df['size'].apply(lambda x: x / K)
    events_df['transfer_time'] = events_df['time.backend'].apply(lambda x: x / K)
    events_df['transfer_speed'] = events_df['size'] / events_df['transfer_time']
    logger.info(f'New cols added:')
    logger.info(print(events_df.loc[:5, ['size', 'time.backend', 'transfer_time', 'transfer_speed']]))
    ### Save preprocessing events csv
    events_df = events_df.loc[:, ['user_id', 'direction', 'size', 'status', 'transfer_time', 'transfer_speed', 'timestamp']]
    logger.info(f'Final events df: {events_df.head(2)}')
    events_df.to_csv(output_path)
    logger.info(f'Final events csv saved at {output_path}.')
    return


if __name__ == '__main__':
    events = list(map(lambda x: str(x), pathlib.Path('shared').glob('*.json')))
    logger.info(f'Retrived {len(events)} events.')
    output_path = 'events_preprocessing.csv'
    preprocess_events(events, output_path)
