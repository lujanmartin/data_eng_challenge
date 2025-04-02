from sqlmodel import SQLModel, Field,UniqueConstraint
from typing import Optional
from datetime import date

class DimDate(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("date"),)  # Prevent duplicate dates
    date_id: Optional[int] = Field(default=None, primary_key=True)
    date: date
    year: int
    month: int
    day: int
    quarter: int