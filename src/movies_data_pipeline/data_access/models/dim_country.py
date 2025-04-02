from sqlmodel import SQLModel, Field, UniqueConstraint
from typing import Optional

class DimCountry(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("country"),)  # Prevent duplicate countries
    country_id: Optional[int] = Field(default=None, primary_key=True)
    country: str