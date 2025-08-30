import os
from typing import List
from ...core.settings import settings

class NBASettings:
    """
    Centralized settings for NBA Analytics App
    """
    # For now this is hardcoded, but could be made dynamic in the future
    DEFAULT_SEASON: str = "2024-25"

    # Same here
    DEFAULT_SEASONS_LIST: List[str] = [
        "2022-23",
        "2023-24",
        "2024-25",
    ]

    @staticmethod
    def get_s3_data_bucket() -> str:
        """ Get S3 bucket name based on environment """
        bucket_name = os.getenv("S3_NBA_DATA_BUCKET_NAME", "nba-analytics-data")

        return f"{bucket_name}-{settings.environment.value}"
