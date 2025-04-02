from sqlmodel import SQLModel, Field,UniqueConstraint
from typing import Optional

class DimMovie(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("name"),)  # Prevent duplicate movie names
    movie_id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    orig_title: str
    overview: str
    status: str
    date_id: int = Field(foreign_key="dimdate.date_id")
    is_deleted: bool = Field(default=False)  