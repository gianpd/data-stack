from http.client import responses
import os
import sys
import pathlib
import logging
logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                level=logging.INFO, datefmt=None)
logger = logging.getLogger("data-ingestion")

from fastapi import APIRouter, HTTPException, File, UploadFile

from io import BytesIO

import pandas as pd

# from app.models.pydantic import DataIngest

from app.pipeline import DataIngestPipeline

pipe = DataIngestPipeline()

router = APIRouter()

@router.post("/", response_model=None, status_code=201)
async def ingest_data(file: UploadFile = File(...)) -> None:
    logger.info('Data ingest POST received')
    fname = file.filename
    if not '.csv' in fname:
        logger.error('data must be in csv format.')
        raise HTTPException(status_code=401, detail=f'data must be in csv format: {fname}')
    contents = await file.read()
    buffer = BytesIO(contents)
    response_error = pipe.ingest(buffer)
    if response_error:
        raise HTTPException(status_code=406, detail=response_error['detail'])
    
    
