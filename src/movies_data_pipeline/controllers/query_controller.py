from fastapi import APIRouter, Depends
from sqlmodel import Session
from typing import List, Dict, Any, Optional
from movies_data_pipeline.data_access.database import get_session
from movies_data_pipeline.services.query_service import QueryService
from movies_data_pipeline.domain.models.movie import Movie

class QueryController:
    def __init__(self):
        self.router = APIRouter()

        @self.router.get("/movies", response_model=List[Dict[str, Any]])
        async def get_movies(
            country: Optional[str] = None,
            language: Optional[str] = None,
            min_score: Optional[float] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            limit: int = 10,
            offset: int = 0,
            session: Session = Depends(get_session)
        ):
            """
            Fetch movies from the data warehouse.
            Optionally filter by country, language, min_score, start_date, and end_date (YYYY-MM-DD format).
            Supports pagination with limit and offset.
            """
            query_service = QueryService()
            movies = query_service.get_movies(
                session, country, language, min_score, start_date, end_date, limit, offset
            )
            return [movie.to_dict() for movie in movies]