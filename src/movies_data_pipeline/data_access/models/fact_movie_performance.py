from sqlmodel import SQLModel, Field
from typing import Optional

class FactMoviePerformance(SQLModel, table=True):
    movie_performance_id: Optional[int] = Field(default=None, primary_key=True)
    movie_id: int = Field(foreign_key="dimmovie.movie_id")
    date_id: int = Field(foreign_key="dimdate.date_id")
    country_id: int = Field(foreign_key="dimcountry.country_id")
    language_id: int = Field(foreign_key="dimlanguage.language_id")
    budget: float
    revenue: float
    score: float
    profit: float