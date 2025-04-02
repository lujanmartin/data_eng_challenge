from fastapi import APIRouter, HTTPException
from typing import Dict, Any
from movies_data_pipeline.services.seed_service import SeedService

class SeedController:
    def __init__(self):
        self.router = APIRouter()
        self.seed_service = SeedService()
        self.router.add_api_route("/seed", self.seed_data, methods=["POST"])

    async def seed_data(self) -> Dict[str, Any]:
        """
        Seed sample data into PostgreSQL and Typesense.
        """
        try:
            result = self.seed_service.seed_sample_data()
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to seed sample data: {str(e)}")

