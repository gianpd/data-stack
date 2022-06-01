from fastapi import FastAPI
from app.core_api import router as ingest_router
from app.utils import logger

from app.db import init_db

def create_application() -> FastAPI:
    app = FastAPI()
    app.include_router(ingest_router, prefix='/ingest', tags=['ingest'])
    return app

app = create_application()

@app.on_event("startup")
async def startup_event():
    logger.info("Starting up ...")
    init_db(app)


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down ...")