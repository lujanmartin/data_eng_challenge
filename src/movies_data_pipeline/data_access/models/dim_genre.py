from sqlmodel import SQLModel, Field,UniqueConstraint
from typing import Optional

class DimGenre(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("genre"),)  # Prevent duplicate genres
    genre_id: Optional[int] = Field(default=None, primary_key=True)
    genre: str