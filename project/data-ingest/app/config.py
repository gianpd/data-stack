import os
import sys
from functools import lru_cache
from pydantic import AnyUrl, BaseSettings

from app.utils import logger


class Settings(BaseSettings):
    environment: str = os.getenv("ENVIRONMENT", "dev")
    testing: bool = os.getenv("TESTING", 0)
    database_url: AnyUrl = os.getenv("DATABASE_URL")

# lru_cache: save the setting values in memory avoiding to re-download they for each request.
@lru_cache
def get_settings() -> BaseSettings:
    logger.info("Loading config settings from the environment ...")
    return Settings()