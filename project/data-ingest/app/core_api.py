from calendar import c
import os
import sys
import json
import pathlib
import logging
from typing import Dict

from fastapi import APIRouter, HTTPException, File, UploadFile, Request

from app.pipeline import DataIngestPipeline
from app.utils import logger

pipe = DataIngestPipeline()

router = APIRouter()

from ast import literal_eval
from functools import lru_cache, wraps
from datetime import datetime, timedelta

def timed_lru_cache(seconds: int, maxsize: int =128):
    """
    Decorator function: allows to save on a LRU cache the received events till the expire time is over.

    The decorated function will save the cache events to the disk if the expire time is over.
    """
    def wrapper_cache(func):
        func = lru_cache(maxsize=maxsize)(func)
        func.lifetime = timedelta(seconds=seconds)
        func.expiration = datetime.utcnow() + func.lifetime
        @wraps(func)
        def wrapped_func(*args, **kwargs):
            if datetime.utcnow() >= func.expiration:
                func.cache_clear()
                func.expiration = datetime.utcnow() + func.lifetime
                return func(*args, **kwargs) # save events to disk
            return {"Response": "in cache"} # no expire time: keep events on cache
        return wrapped_func
    return wrapper_cache

@timed_lru_cache(seconds=10)
def ingest(cache: str):
    """
    Decorated function must save the recorder events on disk when the expire time is over.

    It must receive string *args because the LRU cache needs to keep elements on an hash table.
    Therefore objects must be hashable (have the __hash__ method).
    """
    logger.info(f'Loading events on disk ...')
    cache = literal_eval(cache)
    for _ in range(len(cache)):
        pipe.ingest_raw(cache.pop())
    logger.info(f'cache flushed -> current len {len(cache)}...')
    return {"Response": "ingest"}


cache = []

@router.post("/", response_model=None, status_code=201)
async def ingest_data(event: Request):
    """
    CORE_API POST ingest method: exposed POST method for receiving events raw data.
    """
    global cache
    try:
        event_dict = await event.json()
        # logger.info(f'Received event: {type(event_dict), event_dict}')
    except json.JSONDecodeError as e:
        logger.error(e)
        raise HTTPException(status_code=500, detail=str(e))

    ### check if event contains the wanted fields
    error_field = pipe.event_check(event_dict)
    if error_field:
        raise HTTPException(status_code=406, detail=f"{error_field} must be present in event.")
    
    # append to cache received event -> when time is expired flush the cache and save to disk
    cache.append(event_dict)
    return ingest(str(cache))

    




# @router.post("/", response_model=None, status_code=201)
# async def ingest_data(file: UploadFile = File(...)) -> None:
#     logger.info('Data ingest POST received')
#     fname = file.filename
#     if not '.csv' in fname:
#         logger.error('data must be in csv format.')
#         raise HTTPException(status_code=401, detail=f'data must be in csv format: {fname}')
#     contents = await file.read()
#     buffer = BytesIO(contents)
#     response_error = pipe.ingest(buffer)
#     if response_error:
#         raise HTTPException(status_code=406, detail=response_error['detail'])

    
    
