import os
import sys

from tortoise import Tortoise, run_async, connections

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

async def run() -> None:
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
    client = connections.get("default")
    result = await client.execute_query("SELECT * FROM Events")
    logger.info(result)



if __name__ == "__main__":
    run_async(run())