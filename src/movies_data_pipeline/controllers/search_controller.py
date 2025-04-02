from fastapi import APIRouter
from typing import List, Dict, Any
from movies_data_pipeline.services.search_service import SearchService
from movies_data_pipeline.domain.models.movie import Movie

class SearchController:
    def __init__(self):
        self.router = APIRouter()
        self.search_service = SearchService()

        @self.router.get("/movies", response_model=List[Dict[str, Any]])
        async def search_movies(
            query: str,
            limit: int = 10,
            offset: int = 0
        ):
            """
            Search movies using Typesense.
            - query: Search term to match against name, overview, genres, country, or language.
            - limit: Number of results to return (default: 10).
            - offset: Number of results to skip for pagination (default: 0).
            """
            movies = self.search_service.search_movies(query, limit, offset)
            return [movie.to_dict() for movie in movies]