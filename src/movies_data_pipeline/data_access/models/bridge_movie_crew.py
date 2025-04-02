from sqlmodel import SQLModel, Field
from typing import Optional

class BridgeMovieCrew(SQLModel, table=True):
    movie_crew_id: Optional[int] = Field(default=None, primary_key=True)
    movie_id: int = Field(foreign_key="dimmovie.movie_id")
    crew_id: int = Field(foreign_key="dimcrew.crew_id")
    role_id: int = Field(foreign_key="dimrole.role_id")
    character_name: Optional[str] = None