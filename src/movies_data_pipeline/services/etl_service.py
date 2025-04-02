import pandas as pd
import os
from datetime import datetime
from sqlmodel import Session, select
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
from movies_data_pipeline.domain.models.movie import Movie
import json
import csv
from io import StringIO
import logging
import re
from typing import Tuple, Dict, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Reduce verbosity of sqlalchemy.engine logging
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

class ETLService:
    def __init__(self):
        self.bronze_path = "src/movies_data_pipeline/data_access/data_lake/bronze/movies.parquet"
        self.silver_path = "src/movies_data_pipeline/data_access/data_lake/silver/movies.parquet"
        self.gold_path = "src/movies_data_pipeline/data_access/data_lake/gold/movies.parquet"

    def extract(self, file_path: str) -> pd.DataFrame:
        """
        Extract data from the source file (JSON, CSV, or PDF) in the bronze layer and store as Parquet in bronze layer.

        Args:
            file_path (str): Path to the raw file in the bronze layer.

        Returns:
            pd.DataFrame: The extracted data as a DataFrame.
        """
        logger.info(f"Extracting data from: {file_path}")

        if file_path.endswith(".json"):
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, list):
                raise ValueError("JSON must be a list of movie objects")
            df = pd.DataFrame(data)

        elif file_path.endswith(".csv"):
            df = pd.read_csv(file_path)

        elif file_path.endswith(".pdf"):
            # Parse PDF file
            with open(file_path, "rb") as f:
                content = f.read()
            parsed_data = self.extract_text_from_pdf(content)
            df = pd.DataFrame(parsed_data)

        else:
            raise ValueError(f"Unsupported file type for {file_path}. Expected .json, .csv, or .pdf")

        # Save the DataFrame as Parquet in the bronze layer
        os.makedirs(os.path.dirname(self.bronze_path), exist_ok=True)
        df.to_parquet(self.bronze_path)
        return df

    def transform(self) -> pd.DataFrame:
        """
        Transform the bronze data and store in silver layer.
        Includes renaming columns to match the expected schema and validating required columns.
        """
        df = pd.read_parquet(self.bronze_path)

        # Rename columns to match the expected format
        df = df.rename(columns={
            "names": "name",
            "date_x": "release_date",
            "budget_x": "budget"
        })

        # Validate required columns
        required_columns = ["name", "release_date", "score", "genre", "overview", "crew", "orig_title", "status", "orig_lang", "budget", "revenue", "country"]
        if not all(col in df.columns for col in required_columns):
            missing = [col for col in required_columns if col not in df.columns]
            raise ValueError(f"Data missing required columns: {missing}")

        # Basic cleaning: drop rows with missing required fields
        df = df.dropna(subset=required_columns)

        # Clean the release_date column by stripping all whitespace characters
        df["release_date"] = df["release_date"].astype(str).apply(lambda x: re.sub(r'\s+', '', x))
        logger.info(f"Release dates after cleaning: {df['release_date'].tolist()}")

        # Convert release_date to datetime with error handling
        df["release_date"] = pd.to_datetime(
            df["release_date"],
            format="%m/%d/%Y",
            errors="coerce"  # Convert invalid dates to NaT (Not a Time)
        )

        # Check for rows where date parsing failed
        invalid_dates = df[df["release_date"].isna()]
        if not invalid_dates.empty:
            logger.error(f"Rows with invalid release dates: {invalid_dates[['name', 'release_date']].to_dict('records')}")
            raise ValueError("Some release dates could not be parsed. Check the logs for details.")

        # Split genres into a list
        df["genre"] = df["genre"].apply(lambda x: x.split(", ") if isinstance(x, str) else [])
        # Split crew into a list of name-character pairs
        df["crew"] = df["crew"].apply(lambda x: x.split(", ") if isinstance(x, str) else [])
        os.makedirs(os.path.dirname(self.silver_path), exist_ok=True)
        df.to_parquet(self.silver_path)
        return df

    def load(self, session: Session) -> Tuple[int, int]:
        """
        Load the silver data into the data warehouse and store in gold layer.
        Skips movies that already exist in the dimmovie table to avoid UniqueViolation errors.

        Args:
            session (Session): The SQLModel session for database operations.

        Returns:
            Tuple[int, int]: A tuple of (processed_movies, skipped_movies).
        """
        skipped_movies = 0
        processed_movies = 0

        try:
            df = pd.read_parquet(self.silver_path)

            for _, row in df.iterrows():
                # Check if a movie with this name already exists
                movie_statement = select(DimMovie).where(DimMovie.name == row["name"])
                existing_movie = session.exec(movie_statement).first()

                if existing_movie:
                    logger.warning(f"Skipping movie '{row['name']}' because it already exists in the dimmovie table.")
                    skipped_movies += 1
                    continue

                # Create or link to DimDate
                release_date = row["release_date"].date()
                date_statement = select(DimDate).where(DimDate.date == release_date)
                date_entry = session.exec(date_statement).first()
                if not date_entry:
                    year, month, day = release_date.year, release_date.month, release_date.day
                    quarter = (month - 1) // 3 + 1
                    date_entry = DimDate(date=release_date, year=year, month=month, day=day, quarter=quarter)
                    session.add(date_entry)
                    session.flush()

                # Create or link to DimCountry
                country_statement = select(DimCountry).where(DimCountry.country == row["country"])
                country_entry = session.exec(country_statement).first()
                if not country_entry:
                    country_entry = DimCountry(country=row["country"])
                    session.add(country_entry)
                    session.flush()

                # Create or link to DimLanguage
                language_statement = select(DimLanguage).where(DimLanguage.language == row["orig_lang"])
                language_entry = session.exec(language_statement).first()
                if not language_entry:
                    language_entry = DimLanguage(language=row["orig_lang"])
                    session.add(language_entry)
                    session.flush()

                # Create DimMovie entry
                movie_entry = DimMovie(
                    name=row["name"],
                    orig_title=row["orig_title"],
                    overview=row["overview"],
                    status=row["status"],
                    date_id=date_entry.date_id,
                    is_deleted=False
                )
                session.add(movie_entry)
                session.flush()

                # Create or link to DimGenre and BridgeMovieGenre
                for genre_name in row["genre"]:
                    genre_statement = select(DimGenre).where(DimGenre.genre == genre_name)
                    genre_entry = session.exec(genre_statement).first()
                    if not genre_entry:
                        genre_entry = DimGenre(genre=genre_name)
                        session.add(genre_entry)
                        session.flush()
                    bridge_genre = BridgeMovieGenre(movie_id=movie_entry.movie_id, genre_id=genre_entry.genre_id)
                    session.add(bridge_genre)

                # Create or link to DimCrew, DimRole, and BridgeMovieCrew
                crew_list = row["crew"]
                for i in range(0, len(crew_list), 2):
                    crew_name = crew_list[i]
                    character_name = crew_list[i + 1] if i + 1 < len(crew_list) else None

                    crew_statement = select(DimCrew).where(DimCrew.name == crew_name)
                    crew_entry = session.exec(crew_statement).first()
                    if not crew_entry:
                        crew_entry = DimCrew(name=crew_name)
                        session.add(crew_entry)
                        session.flush()

                    role_name = "Actor" if character_name else "Unknown"
                    role_type = "Character" if character_name else "Job"
                    role_statement = select(DimRole).where(DimRole.role_name == role_name).where(DimRole.role_type == role_type)
                    role_entry = session.exec(role_statement).first()
                    if not role_entry:
                        role_entry = DimRole(role_name=role_name, role_type=role_type)
                        session.add(role_entry)
                        session.flush()

                    bridge_crew = BridgeMovieCrew(
                        movie_id=movie_entry.movie_id,
                        crew_id=crew_entry.crew_id,
                        role_id=role_entry.role_id,
                        character_name=character_name
                    )
                    session.add(bridge_crew)

                # Create FactMoviePerformance entry
                profit = float(row["revenue"]) - float(row["budget"])
                performance_entry = FactMoviePerformance(
                    movie_id=movie_entry.movie_id,
                    date_id=date_entry.date_id,
                    country_id=country_entry.country_id,
                    language_id=language_entry.language_id,
                    budget=float(row["budget"]),
                    revenue=float(row["revenue"]),
                    score=float(row["score"]),
                    profit=profit
                )
                session.add(performance_entry)

                processed_movies += 1

            session.commit()
            # Store in gold layer (for now, just copy the silver data)
            os.makedirs(os.path.dirname(self.gold_path), exist_ok=True)
            df.to_parquet(self.gold_path)

        except Exception as e:
            logger.error(f"Error in load method: {str(e)}")
            session.rollback()  # Roll back the transaction on error
            raise  # Re-raise the exception to be caught by the caller

        finally:
            logger.info(f"Processed {processed_movies} new movies, skipped {skipped_movies} duplicates.")
            return processed_movies, skipped_movies

    def extract_text_from_pdf(self, content: bytes) -> list:
        """
        Placeholder function to extract movie data from a PDF.
        Implement using a library like PyPDF2 or pdfplumber.
        """
        raise NotImplementedError("PDF parsing not implemented")