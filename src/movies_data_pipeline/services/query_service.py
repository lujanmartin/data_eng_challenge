from typing import List
from sqlmodel import Session, select
from datetime import datetime
from movies_data_pipeline.data_access.models.fact_movie_performance import FactMoviePerformance
from movies_data_pipeline.data_access.models.dim_movie import DimMovie
from movies_data_pipeline.data_access.models.dim_date import DimDate
from movies_data_pipeline.data_access.models.dim_genre import DimGenre
from movies_data_pipeline.data_access.models.dim_crew import DimCrew
from movies_data_pipeline.data_access.models.dim_country import DimCountry
from movies_data_pipeline.data_access.models.dim_language import DimLanguage
from movies_data_pipeline.data_access.models.bridge_movie_genre import BridgeMovieGenre
from movies_data_pipeline.data_access.models.bridge_movie_crew import BridgeMovieCrew
from movies_data_pipeline.domain.models.movie import Movie

class QueryService:
    def get_movies(
        self,
        session: Session,
        country: str = None,
        language: str = None,
        min_score: float = None,
        start_date: str = None,
        end_date: str = None,
        limit: int = 10,
        offset: int = 0
    ) -> List[Movie]:
        """
        Fetch movies from the data warehouse.
        Optionally filter by country, language, min_score, start_date, and end_date.
        Supports pagination with limit and offset.
        """
        statement = (
            select(FactMoviePerformance, DimMovie, DimDate, DimCountry, DimLanguage)
            .join(DimMovie, FactMoviePerformance.movie_id == DimMovie.movie_id)
            .join(DimDate, FactMoviePerformance.date_id == DimDate.date_id)
            .join(DimCountry, FactMoviePerformance.country_id == DimCountry.country_id)
            .join(DimLanguage, FactMoviePerformance.language_id == DimLanguage.language_id)
            .where(DimMovie.is_deleted == False)
        )

        if country:
            statement = statement.where(DimCountry.country.ilike(f"%{country}%"))
        if language:
            statement = statement.where(DimLanguage.language.ilike(f"%{language}%"))
        if min_score is not None:
            statement = statement.where(FactMoviePerformance.score >= min_score)
        if start_date:
            try:
                start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
                statement = statement.where(DimDate.date >= start_date_obj)
            except ValueError:
                raise ValueError("start_date must be in 'YYYY-MM-DD' format")
        if end_date:
            try:
                end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
                statement = statement.where(DimDate.date <= end_date_obj)
            except ValueError:
                raise ValueError("end_date must be in 'YYYY-MM-DD' format")

        statement = statement.offset(offset).limit(limit)
        results = session.exec(statement).all()

        movies = []
        for performance, movie, date, country, language in results:
            # Fetch genres
            genre_statement = (
                select(DimGenre)
                .join(BridgeMovieGenre, BridgeMovieGenre.genre_id == DimGenre.genre_id)
                .where(BridgeMovieGenre.movie_id == performance.movie_id)
            )
            genres = session.exec(genre_statement).all()
            genre_list = [g.genre for g in genres]

            # Fetch crew
            crew_statement = (
                select(DimCrew, BridgeMovieCrew)
                .join(BridgeMovieCrew, BridgeMovieCrew.crew_id == DimCrew.crew_id)
                .where(BridgeMovieCrew.movie_id == performance.movie_id)
            )
            crew_results = session.exec(crew_statement).all()
            crew_list = [{"name": c.name, "role_name": "Actor", "character_name": bc.character_name} for c, bc in crew_results]

            movie_domain = Movie(
                name=movie.name,
                orig_title=movie.orig_title,
                overview=movie.overview,
                status=movie.status,
                release_date=date.date,
                genres=genre_list,
                crew=crew_list,
                country=country.country,
                language=language.language,
                budget=performance.budget,
                revenue=performance.revenue,
                score=performance.score,
                is_deleted=movie.is_deleted
            )
            movies.append(movie_domain)
        return movies