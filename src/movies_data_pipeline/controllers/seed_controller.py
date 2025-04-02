from fastapi import APIRouter, UploadFile, File, HTTPException
from typing import Dict, Any
from movies_data_pipeline.services.seed_service import SeedService
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SeedController:
    def __init__(self):
        self.router = APIRouter()
        self.seed_service = SeedService()
        self.router.add_api_route("/seed", self.seed_data, methods=["POST"])
        self.router.add_api_route("/seed-from-csv", self.seed_data_from_csv, methods=["POST"])  # New endpoint
        self.router.add_api_route("/seed-from-file", self.seed_from_file, methods=["POST"])  # New endpoint

    async def seed_data(self) -> Dict[str, Any]:
        """
        Seed sample data into PostgreSQL and Typesense.
        """
        try:
            result = self.seed_service.seed_sample_data()
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to seed sample data: {str(e)}")
    
    async def seed_data_from_csv(self) -> Dict[str, Any]:
        """
        Seed sample data from a CSV file into PostgreSQL and Typesense.
        """
        try:
            result = self.seed_service.seed_sample_data_from_csv()
            return result
        except HTTPException as e:
            raise e
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to seed sample data from CSV: {str(e)}")

    
    async def seed_from_file(self, file: UploadFile = File(...), file_type: str = "json") -> Dict[str, Any]:
        """
        Seed movie data from an uploaded file (JSON, CSV, or PDF) into PostgreSQL and Typesense.

        Args:
            file (UploadFile): The uploaded file containing movie data.
            file_type (str): The type of file ('json', 'csv', or 'pdf').

        Returns:
            Dict[str, Any]: A message indicating the number of movies seeded.

        Raises:
            HTTPException: If the file format is invalid or unsupported.
        """
        try:
            # Validate file type
            allowed_types = {"json", "csv", "pdf"}
            if file_type not in allowed_types:
                raise HTTPException(status_code=400, detail=f"File type must be one of {allowed_types}")

            # Delegate to SeedService
            result = await self.seed_service.seed_from_file(file, file_type)
            return result

        except HTTPException as e:
            raise e
        except Exception as e:
            logger.error(f"Error seeding from file: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error seeding from file: {str(e)}")