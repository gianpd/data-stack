import os
import sys
import pathlib
import logging
logging.basicConfig(stream=sys.stdout, format='%(asctime)-15s %(message)s',
                level=logging.INFO, datefmt=None)
logger = logging.getLogger("data-ingestion-pipeline")

from dataclasses import dataclass

from io import BytesIO
import pandas as pd


from typing import List, Dict, Optional


@dataclass
class DataIngestPipeline:
    def ingest(self, buffer: BytesIO) -> Dict:
        try:
            df = pd.read_csv(buffer)
            logger.info(df.head(2))
            return None
        except pd.errors.EmptyDataError as e:
            logger.error(e)
            return dict(detail=str(e))