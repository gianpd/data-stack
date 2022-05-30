import os
import sys
import json
import pathlib
import logging
logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                level=logging.INFO, datefmt=None)
logger = logging.getLogger("data-ingestion")

from io import BytesIO
from fastapi import APIRouter, HTTPException, File, UploadFile, Request
import pandas as pd

from app.pipeline import DataIngestPipeline

pipe = DataIngestPipeline()

router = APIRouter()

@router.post("/", response_model=None, status_code=201)
async def ingest_data(event: Request):
    """
    CORE_API POST ingest method: exposed POST method for receiving events raw data.
    """
    try:
        event_dict = await event.json()
        logger.info(f'Received event: {event_dict}')
    except json.JSONDecodeError as e:
        logger.error(e)
        raise HTTPException(status_code=500, detail=str(e))

    ### check if event contains the wanted fields
    error_field = pipe.event_check(event_dict)
    if error_field:
        raise HTTPException(status_code=406, detail=f"{error_field} must be present in event.")
    await pipe.ingest_raw(event_dict)
    logger.info(f'POST/ingest/ precessed correctly.')



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

    
    
