from sqlmodel import SQLModel, Field
from typing import Optional

class DimCrew(SQLModel, table=True):
    crew_id: Optional[int] = Field(default=None, primary_key=True)
    name: str