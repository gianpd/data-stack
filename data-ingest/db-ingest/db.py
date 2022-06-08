import os
import sys

from typing import Union, List

from tortoise import Tortoise, run_async

import crud
from models.tortoise import Events_pydantic, Users_pydantic

import pandas as pd

import logging
logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                level=logging.INFO, datefmt=None)
logger = logging.getLogger("db-ingest")

TORTOISE_ORM = {
    "connections": {"default": os.environ.get("DATABASE_URL")},
    "apps": {
        "models": {
            "models": ["models.tortoise", "aerich.models"],
            "default_connection": "default",
        },
    },
}

async def run_post(events: Union[List[Events_pydantic], List[Users_pydantic]], users=False) -> None:
    """
    Asynch method for uploading rows to both Users and Events DB table. It is called via run_async
    tortoise method, which is a context manager, closing the connection when it's done.

    ---Parameters
      -events: the pydantic objects as expected by the ORM model (list of pydantic objs)
      -users: bool (the type of the event: Users or Events)

    return None
    """
    logger.info("Initializing Tortoise ...")
    db_url = os.environ.get('DATABASE_URL')
    logger.debug(f'Trying to connect to {db_url} ...')
    await Tortoise.init(config=TORTOISE_ORM)
    logger.info("Generating database schema via Tortoise ...")
    await Tortoise.generate_schemas()
    for event in events:
        i = await crud.post(event, users=users)
        logger.info(f'Events {i} created')
    return



if __name__ == "__main__":
    output_path = 'events_preprocessing.csv'
    logger.info('Reading preprocessed events df:')
    df = pd.read_csv(output_path, index_col=0)
    logger.info(df.head())

    event = dict(df.iloc[0])
    logger.info(f'events DB schema: {Events_pydantic.schema_json(indent=4)}')
    event = Events_pydantic.parse_obj(event)
    logger.info(f'Trying to upload event: {event} to Events Table')
    logger.info(f'Events pydantic {event}')
    run_async(run_post(event))