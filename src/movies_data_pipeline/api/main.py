from fastapi import FastAPI
from movies_data_pipeline.api.routes.data import router as data_router
from movies_data_pipeline.api.routes.query import router as query_router
from movies_data_pipeline.api.routes.search import router as search_router
from movies_data_pipeline.data_access.database import init_db
from movies_data_pipeline.data_access.vector_db import VectorDB

app = FastAPI()

# Initialize database and vector DB on startup
@app.on_event("startup")
async def startup_event():
    init_db()  # Create PostgreSQL tables
    vector_db = VectorDB()
    vector_db.create_collection()  # Create Typesense collection

# Include routers
app.include_router(data_router, prefix="/v0.1.0")
app.include_router(query_router, prefix="/v0.1.0")
app.include_router(search_router, prefix="/v0.1.0")