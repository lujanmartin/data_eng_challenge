from typing import List, Optional
from datetime import date

class Movie:
    def __init__(
        self,
        name: str,
        orig_title: str,
        overview: str,
        status: str,
        release_date: date,
        genres: List[str],
        crew: List[dict],  # List of {"name": str, "role_name": str, "character_name": Optional[str]}
        country: str,
        language: str,
        budget: float,
        revenue: float,
        score: float,
        is_deleted: bool = False
    ):
        self.name = name
        self.orig_title = orig_title
        self.overview = overview
        self.status = status
        self.release_date = release_date
        self.genres = genres
        self.crew = crew
        self.country = country
        self.language = language
        self.budget = budget
        self.revenue = revenue
        self.score = score
        self.is_deleted = is_deleted

    def calculate_profit(self) -> float:
        """Calculate the profit of the movie."""
        return self.revenue - self.budget

    def is_profitable(self) -> bool:
        """Check if the movie made a profit."""
        return self.calculate_profit() > 0

    def mark_as_deleted(self) -> None:
        """Mark the movie as deleted (soft deletion)."""
        self.is_deleted = True

    def to_dict(self) -> dict:
        """Convert the Movie domain model to a dictionary for serialization."""
        return {
            "name": self.name,
            "orig_title": self.orig_title,
            "overview": self.overview,
            "status": self.status,
            "release_date": self.release_date.isoformat() if self.release_date else None,
            "genres": self.genres,
            "crew": self.crew,
            "country": self.country,
            "language": self.language,
            "budget": self.budget,
            "revenue": self.revenue,
            "score": self.score,
            "profit": self.calculate_profit(),
            "is_deleted": self.is_deleted
        }