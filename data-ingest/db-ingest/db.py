import os
import sys

from tortoise import Tortoise, run_async, connections

import crud
from models.tortoise import Events_pydantic

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

async def run(event: Events_pydantic, users=False) -> None:
    """
    Applying the schema to the database in its final state and connect to it.
    """
    logger.info("Initializing Tortoise ...")
    db_url = os.environ.get('DATABASE_URL')
    logger.debug(f'Trying to connect to {db_url} ...')
    # await Tortoise.init(db_url=os.environ.get('DATABASE_URL'), modules={"models": ["models.tortoise"]})
    await Tortoise.init(config=TORTOISE_ORM)
    logger.info("Generating database schema via Tortoise ...")
    await Tortoise.generate_schemas()
    i = await crud.post(event, users=users)
    logger.info(f'Events {i} created')



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
    run_async(run(event))