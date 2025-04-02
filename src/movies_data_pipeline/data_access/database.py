import os
from sqlmodel import create_engine, SQLModel, Session
from sqlalchemy import create_engine as sqlalchemy_create_engine
from sqlalchemy.sql import text

DATABASE_URL = os.getenv("DATABASE_URL")
print(f"DEBUG: DATABASE_URL={DATABASE_URL}")

# Extract the database name from the DATABASE_URL
db_name = DATABASE_URL.split("/")[-1]

# Create a connection to the default 'postgres' database to check/create the target database
default_db_url = DATABASE_URL.replace(f"/{db_name}", "/postgres")
default_engine = sqlalchemy_create_engine(default_db_url)

# Check if the database exists, and create it if it doesn't
with default_engine.connect() as conn:
  conn.execute(text("COMMIT"))  # Ensure we're not in a transaction
  result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'"))
  if not result.fetchone():
      conn.execute(text(f"CREATE DATABASE {db_name}"))
      print(f"Created database: {db_name}")
  else:
      print(f"Database {db_name} already exists")

# Now create the engine for the target database
engine = create_engine(DATABASE_URL, echo=True)

def init_db():
  SQLModel.metadata.create_all(engine)

def get_session():
    """
    Generator function for FastAPI dependency injection.
    """
    with Session(engine) as session:
        yield session

def get_session_direct():
    """
    Returns a Session object directly for use in non-FastAPI contexts.
    """
    return Session(engine)