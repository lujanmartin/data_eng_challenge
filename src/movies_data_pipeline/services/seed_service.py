import pandas as pd
from typing import Dict, Any
from datetime import datetime
from sqlmodel import Session, select
from fastapi import UploadFile, HTTPException
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
import logging
import uuid
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Reduce verbosity of sqlalchemy.engine logging
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

class SeedService:
    def __init__(self):
        self.etl_service = ETLService()
        self.search_service = SearchService()
        # Define the base path for the bronze layer
        self.bronze_base_path = Path("src/movies_data_pipeline/data_access/data_lake/bronze")
        # Define the path to the sample CSV file
        self.sample_csv_path = Path("src/movies_data_pipeline/utils/imdb_movies.csv")

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
        bronze_path = self.bronze_base_path / "sample_movies.json"
        bronze_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_json(bronze_path, orient="records")

        # Use ETLService to process the data
        self.etl_service.extract(str(bronze_path))
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
                "budget": 75000000.0,
                "revenue": 271616668.0
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
                "budget": 460000000.0,
                "revenue": 2316794914.0
            }
        ]
        for movie in movies:
            print(movie)
            self.search_service.index_movie(movie)
        
        return {"message": "Sample data seeded successfully"}

    def seed_sample_data_from_csv(self) -> dict:
        """
        Seed sample data from a CSV file located in the utils folder into the data lake, PostgreSQL, and Typesense.
        """
        try:
            # Check if the CSV file exists
            if not self.sample_csv_path.exists():
                raise HTTPException(status_code=404, detail=f"Sample CSV file not found at: {self.sample_csv_path}")

            # Generate a unique file name for the bronze layer
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_id = str(uuid.uuid4())
            bronze_filename = f"sample_movies_{timestamp}_{unique_id}.csv"
            bronze_path = self.bronze_base_path / bronze_filename

            # Copy the CSV file to the bronze layer
            bronze_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.sample_csv_path, "rb") as src_file, open(bronze_path, "wb") as dst_file:
                dst_file.write(src_file.read())
            logger.info(f"Copied sample CSV to bronze layer: {bronze_path}")

            # Use ETLService to process the CSV file
            self.etl_service.extract(str(bronze_path))
            transformed_df = self.etl_service.transform()
            with get_session_direct() as session:
                processed, skipped = self.etl_service.load(session)

            # Index movies in Typesense using the transformed DataFrame
            movies_to_index = []
            for idx, row in transformed_df.iterrows():
                # Skip movies that were not loaded into PostgreSQL (duplicates)
                movie_statement = select(DimMovie).where(DimMovie.name == row["name"])
                existing_movie = session.exec(movie_statement).first()
                if not existing_movie:
                    continue  # Skip if the movie was not inserted (duplicate)

                movie_dict = {
                    "id": str(existing_movie.movie_id),  # Use the movie_id from PostgreSQL
                    "name": row["name"],
                    "overview": row["overview"],
                    "score": float(row["score"]),
                    "genres": row.get("genre", "").split(", ") if isinstance(row.get("genre"), str) else [],
                    "country": row["country"],
                    "language": row["orig_lang"],
                    "release_date": row["release_date"].strftime("%Y-%m-%d") if isinstance(row["release_date"], (pd.Timestamp, datetime)) else row["release_date"],
                    "status": row["status"],
                    "budget": float(row["budget"]),
                    "revenue": float(row["revenue"])
                }
                movies_to_index.append(movie_dict)

            for movie in movies_to_index:
                self.search_service.index_movie(movie)

            logger.info(f"Processed {processed} new movies, skipped {skipped} duplicates from CSV file")
            return {"message": f"Processed {processed} new movies, skipped {skipped} duplicates from CSV file"}

        except Exception as e:
            logger.error(f"Error seeding from CSV file: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error seeding from CSV file: {str(e)}")


    async def seed_from_file(self, file: UploadFile, file_type: str) -> Dict[str, Any]:
        """
        Seed movie data from an uploaded file (JSON, CSV, or PDF) into PostgreSQL and Typesense.

        Args:
            file (UploadFile): The uploaded file containing movie data.
            file_type (str): The type of file ('json', 'csv', or 'pdf').

        Returns:
            Dict[str, Any]: A message indicating the number of movies seeded.

        Raises:
            HTTPException: If the file format is invalid or unsupported.
        """
        try:
            # Validate file type
            allowed_types = {"json", "csv", "pdf"}
            if file_type not in allowed_types:
                raise HTTPException(status_code=400, detail=f"File type must be one of {allowed_types}")

            # Generate a unique file name using a timestamp and UUID
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_id = str(uuid.uuid4())
            bronze_filename = f"uploaded_movies_{timestamp}_{unique_id}.{file_type}"
            bronze_path = self.bronze_base_path / bronze_filename

            # Save the raw file to the bronze layer
            bronze_path.parent.mkdir(parents=True, exist_ok=True)
            with open(bronze_path, "wb") as f:
                content = await file.read()
                f.write(content)
            logger.info(f"Saved raw uploaded file to: {bronze_path}")

            # Use ETLService to process the raw file
            df = self.etl_service.extract(str(bronze_path))
            self.etl_service.transform()
            with get_session_direct() as session:
                self.etl_service.load(session)

            # Index movies in Typesense
            movies_to_index = []
            for idx, row in df.iterrows():
                movie_dict = {
                    "id": str(idx + 1),  # Use index as ID (or use row["id"] if provided)
                    "name": row["name"],
                    "overview": row["overview"],
                    "score": float(row["score"]),
                    "genres": row.get("genre", "").split(", ") if isinstance(row.get("genre"), str) else [],
                    "country": row["country"],
                    "language": row["orig_lang"],
                    "release_date": row["release_date"].strftime("%Y-%m-%d") if isinstance(row["release_date"], (pd.Timestamp, datetime)) else row["release_date"],
                    "status": row["status"],
                    "budget": float(row["budget"]),
                    "revenue": float(row["revenue"])
                }
                movies_to_index.append(movie_dict)

            for movie in movies_to_index:
                self.search_service.index_movie(movie)

            logger.info(f"Seeded {len(df)} movies from file")
            return {"message": f"Seeded {len(df)} movies from file"}

        except Exception as e:
            logger.error(f"Error seeding from file: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error seeding from file: {str(e)}")

    def extract_text_from_pdf(self, content: bytes) -> list:
        """
        Placeholder function to extract movie data from a PDF.
        Implement using a library like PyPDF2 or pdfplumber.
        """
        raise NotImplementedError("PDF parsing not implemented")