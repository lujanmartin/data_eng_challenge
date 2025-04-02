from sqlmodel import SQLModel, Field
from typing import Optional

class BridgeMovieGenre(SQLModel, table=True):
    movie_genre_id: Optional[int] = Field(default=None, primary_key=True)
    movie_id: int = Field(foreign_key="dimmovie.movie_id")
    genre_id: int = Field(foreign_key="dimgenre.genre_id")