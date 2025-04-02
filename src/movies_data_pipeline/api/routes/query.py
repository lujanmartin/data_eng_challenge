from fastapi import APIRouter
from movies_data_pipeline.controllers.query_controller import QueryController

router = APIRouter(prefix="/query", tags=["query"])
query_controller = QueryController()
router.include_router(query_controller.router)