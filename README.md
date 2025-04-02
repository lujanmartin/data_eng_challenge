# Data Pipeline for Business Analytics

A data engineering project that implements an ETL pipeline, data lake, data warehouse, vector database, and REST API for movies business analysis.

## Architecture
- **PostgreSQL**: Data warehouse designed with a star schema for efficient querying and analytics.
- **Typesense**: Vector database enabling fast, scalable search capabilities.
- **FastAPI**: REST API for seamless data operations and interaction with the pipeline.
- **Data Lake**: Medallion architecture (bronze, silver, gold layers) using Parquet files for structured data storage and processing.

## Dataset

This project uses the "IMDB movies dataset" from Kaggle, available at: https://www.kaggle.com/datasets/ashpalsingh1525/imdb-movies-dataset

## Project Structure

Below is the directory structure of the project, outlining the key components and their organization:

src/
│
├── movies_data_pipeline/
│   ├── api/
│   │   ├── routes/
│   │   │   ├── data.py              # Routes for seeding data (POST /v0.1.0/data/seed)
│   │   │   ├── query.py             # Routes for querying movies from PostgreSQL (GET /v0.1.0/query/movies)
│   │   │   └── search.py            # Routes for searching movies with Typesense (GET /v0.1.0/search/movies)
│   │   └── main.py                  # FastAPI app entry point
│   ├── controllers/
│   │   ├── seed_controller.py       # Logic for seeding data into PostgreSQL and Typesense
│   │   ├── query_controller.py      # Logic for querying movies from the data warehouse
│   │   └── search_controller.py     # Logic for searching movies using Typesense
│   ├── services/
│   │   ├── etl_service.py           # ETL pipeline for extracting, transforming, and loading movie data
│   │   ├── query_service.py         # Service for querying movies from PostgreSQL
│   │   ├── search_service.py        # Service for searching movies using Typesense
│   │   └── seed_service.py          # Service for seeding sample data
│   ├── data_access/
│   │   ├── models/
│   │   │   ├── fact_movie_performance.py  # Fact table for movie performance metrics
│   │   │   ├── dim_movie.py         # Dimension table for movie details
│   │   │   ├── dim_genre.py         # Dimension table for genres
│   │   │   ├── dim_crew.py          # Dimension table for crew members
│   │   │   ├── dim_country.py       # Dimension table for countries
│   │   │   ├── dim_language.py      # Dimension table for languages
│   │   │   ├── dim_date.py          # Dimension table for dates
│   │   │   ├── bridge_movie_genre.py  # Bridge table for movie-genre relationships
│   │   │   └── bridge_movie_crew.py   # Bridge table for movie-crew relationships
│   │   ├── data_lake/
│   │   │   ├── bronze/              # Raw data layer
│   │   │   ├── silver/              # Cleaned and transformed data layer
│   │   │   └── gold/                # Aggregated and optimized data layer
│   │   ├── database.py              # Database connection and session management for PostgreSQL
│   │   └── vector_db.py             # Configuration and connection for Typesense vector database
│   └── domain/
│       └── models/
│           └── movie.py             # Domain model for movie entities
│
├── Dockerfile                       # Dockerfile for FastAPI app
├── docker-compose.yml               # Docker Compose for all services (PostgreSQL, Typesense, FastAPI)
├── pyproject.toml                   # Poetry for dependency management
└── README.md                        # Project documentation

## Initial Commit (v0.1.0)
The first commit includes a minimal setup for the core components of the pipeline:
- **Data Warehouse**: A star schema with `FactMoviePerformance`, `DimMovie`, `DimGenre`, `DimCrew`, `DimRole`, `DimCountry`, `DimLanguage`, `DimDate`, `BridgeMovieGenre`, and `BridgeMovieCrew`.
- **ETL Pipeline**: Loads sample movie data into the data warehouse, using a medallion architecture (bronze, silver, gold layers).
- **REST API**: Endpoints for querying, searching, and seeding data:
  - **GET `/v0.1.0/query/movies`**: Fetch movies from the PostgreSQL data warehouse. Optionally filter by `country`, `language`, `min_score`, `start_date`, and `end_date` (YYYY-MM-DD format). Supports pagination with `limit` (default: 10) and `offset` (default: 0).
  - **GET `/v0.1.0/search/movies`**: Search movies using Typesense. Query with a search term to match against `name`, `overview`, `genres`, `country`, `language`, or `status`. Supports pagination with `limit` (default: 10) and `offset` (default: 0).
  - **POST `/v0.1.0/data/seed`**: Seed sample data into PostgreSQL and Typesense. No parameters required.
- **Docker Setup**: Runs PostgreSQL, Typesense, and the FastAPI app.

