import pandas as pd
import os
from datetime import datetime
from sqlmodel import Session, select
from movies_data_pipeline.data_access.database import get_session_direct
from movies_data_pipeline.data_access.models.fact_movie_performance import FactMoviePerformance
from movies_data_pipeline.data_access.models.dim_movie import DimMovie
from movies_data_pipeline.data_access.models.dim_date import DimDate
from movies_data_pipeline.data_access.models.dim_genre import DimGenre
from movies_data_pipeline.data_access.models.dim_crew import DimCrew
from movies_data_pipeline.data_access.models.dim_country import DimCountry
from movies_data_pipeline.data_access.models.dim_language import DimLanguage
from movies_data_pipeline.data_access.models.dim_role import DimRole
from movies_data_pipeline.data_access.models.bridge_movie_genre import BridgeMovieGenre
from movies_data_pipeline.data_access.models.bridge_movie_crew import BridgeMovieCrew
from movies_data_pipeline.services.etl_service import ETLService
from movies_data_pipeline.services.search_service import SearchService

class SeedService:
    def __init__(self):
        self.etl_service = ETLService()
        self.search_service = SearchService()

    def seed_sample_data(self) -> dict:
        """Seed sample data into the data lake, PostgreSQL, and Typesense."""

        # Create sample data for two movies
        data = {
            "name": ["Creed III", "Avatar: The Way of Water"],
            "release_date": ["03/02/2023", "12/15/2022"],
            "score": [73.0, 78.0],
            "genre": ["Drama, Action", "Science Fiction, Adventure, Action"],
            "overview": [
                "After dominating the boxing world, Adonis Creed has been thriving in both his career and family life...",
                "Set more than a decade after the events of the first film, learn the story of the Sully family (Jake..."
            ],
            "crew": [
                "Michael B. Jordan, Adonis Creed, Tessa Thompson, Bianca Taylor, Jonathan Majors, Damien Anderson",
                "Sam Worthington, Jake Sully, Zoe Saldaña, Neytiri, Sigourney Weaver, Kiri / Dr. Grace Augustine"
            ],
            "orig_title": ["Creed III", "Avatar: The Way of Water"],
            "status": ["Released", "Released"],
            "orig_lang": ["English", "English"],
            "budget": [75000000.0, 460000000.0],
            "revenue": [271616668.0, 2316794914.0],
            "country": ["AU", "AU"]
        }
        df = pd.DataFrame(data)

        # Save to the bronze layer as JSON
        bronze_path = "src/movies_data_pipeline/data_access/data_lake/bronze/sample_movies.json"
        os.makedirs(os.path.dirname(bronze_path), exist_ok=True)
        df.to_json(bronze_path, orient="records")

        # Use ETLService to process the data
        self.etl_service.extract(bronze_path)
        self.etl_service.transform()
        self.etl_service.load(get_session_direct())        

        # Index in Typesense
        movies = [
            {
                "id": '1',
                "name": "Creed III",
                "overview": "After dominating the boxing world, Adonis Creed has been thriving in both his career and family life...",
                "score": 73.0,
                "genres": ["Drama", "Action"],
                "country": "AU",
                "language": "English",
                "release_date": "2023-03-02",
                "status": "Released",
                "budget" : 75000000.0,
                "revenue" : 271616668.0
            },
            {
                "id": '2',
                "name": "Avatar: The Way of Water",
                "overview": "Set more than a decade after the events of the first film, learn the story of the Sully family (Jake...",
                "score": 78.0,
                "genres": ["Science Fiction", "Adventure", "Action"],
                "country": "AU",
                "language": "English",
                "release_date": "2022-12-15",
                "status": "Released",
                "budget" : 460000000.0,
                "revenue" : 2316794914.0
            }
        ]
        for movie in movies:
            print(movie)
            self.search_service.index_movie(movie)
        
        return {"message": "Sample data seeded successfully"}