import json

from fastapi import APIRouter, HTTPException, File, UploadFile, Request

from pipeline import DataIngestPipeline
from utils import logger, timed_lru_cache

from ast import literal_eval

pipe = DataIngestPipeline()

router = APIRouter()

cache = []

@timed_lru_cache(seconds=10)
def ingest_timed_events(cache: str):
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
    return ingest_timed_events(str(cache))

    




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

    
    
