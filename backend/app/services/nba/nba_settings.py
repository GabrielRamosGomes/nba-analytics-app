import os
from typing import List
from ...core.settings import settings

class NBASettings:
    """
    Centralized settings for NBA Analytics App
    """

    DEFAULT_SEASON: str = "2023-24"

    DEFAULT_SEASONS_LIST: List[str] = [
        "2021-22",
        "2022-23",
        "2023-24"
    ]

    DEFAULT_DATA_FORMAT: str = "csv"

    def __init__(self):
        self.s3_bucket = self.get_s3_bucket()

    @staticmethod
    def get_s3_data_bucket() -> str:
        """ Get S3 bucket name based on environment """
        bucket_name = os.getenv("S3_NBA_DATA_BUCKET_NAME", "nba-analytics-data")

        return f"{bucket_name}-{settings.environment.value}"
