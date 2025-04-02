from sqlmodel import SQLModel, Field, UniqueConstraint
from typing import Optional

class DimLanguage(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("language"),)  # Prevent duplicate languages
    language_id: Optional[int] = Field(default=None, primary_key=True)
    language: str