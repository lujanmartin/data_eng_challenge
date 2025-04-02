import typesense
import os
from typing import Dict, List, Any

class VectorDB:
    def __init__(self):
        # Initialize Typesense client
        self.client = typesense.Client({
            'nodes': [{
                'host': os.getenv("TYPESENSE_HOST", "typesense"),
                'port': os.getenv("TYPESENSE_PORT", "8108"),
                'protocol': 'http'
            }],
            'api_key': os.getenv("TYPESENSE_API_KEY", "xyz"),
            'connection_timeout_seconds': 2
        })
        self.collection_name = "movies"

    def create_collection(self):
        """Create a Typesense collection for movies if it doesn't exist."""
        try:
            # Check if the collection already exists
            self.client.collections[self.collection_name].retrieve()
        except typesense.exceptions.ObjectNotFound:
            # Define the schema for the movies collection
            schema = {
                'name': self.collection_name,
                'fields': [
                    {'name': 'name', 'type': 'string'},
                    {'name': 'overview', 'type': 'string'},
                    {'name': 'genres', 'type': 'string[]'},
                    {'name': 'country', 'type': 'string'},
                    {'name': 'language', 'type': 'string'},
                    {'name': 'score', 'type': 'float'},
                    {'name': 'release_date', 'type': 'string'},  # Store as string for simplicity
                ],
                'default_sorting_field': 'score'
            }
            self.client.collections.create(schema)

    def index_movie(self, movie: Dict[str, Any]):
        """Index a movie in Typesense."""
        try:
            self.client.collections[self.collection_name].documents.create(movie)
        except Exception as e:
            print(f"Error indexing movie: {e}")

    def search_movies(self, query: str, per_page: int = 10, page: int = 1) -> List[Dict[str, Any]]:
        """Search movies in Typesense."""
        search_parameters = {
            'q': query,
            'query_by': 'name,overview,genres,country,language',
            'per_page': per_page,
            'page': page
        }
        try:
            result = self.client.collections[self.collection_name].documents.search(search_parameters)
            return result['hits']
        except Exception as e:
            print(f"Error searching movies: {e}")
            return []