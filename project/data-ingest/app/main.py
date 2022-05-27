from fastapi import FastAPI
from app.core_api import router as ingest_router

def create_application() -> FastAPI:
    app = FastAPI()
    app.include_router(ingest_router, prefix='/ingest', tags=['ingest'])
    return app

app = create_application()