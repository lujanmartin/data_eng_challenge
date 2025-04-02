from sqlmodel import SQLModel, Field, UniqueConstraint
from typing import Optional

class DimRole(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("role_name", "role_type"),)  # Prevent duplicate role combinations
    role_id: Optional[int] = Field(default=None, primary_key=True)
    role_name: str
    role_type: str