import os
from enum import Enum
from pathlib import Path
from typing import List
from dotenv import load_dotenv
from datetime import date

from ..services.storage.s3_storage import S3Storage
from ..services.storage.local_storage import LocalStorage

import logging
logger = logging.getLogger(__name__)

root_dir = Path(__file__).resolve().parent.parent.parent.parent
env_path = root_dir / '.env'

class Settings:
    """
    Centralized application settings
    """

    class Environment(Enum):
        DEV = "dev"
        PROD = "prod"
        TESTING = "test"

    def __init__(self):
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
            logger.info(f"✅ Loaded environment variables from {env_path}")
        else:
            logger.warning(f"⚠️ .env file not found at {env_path}. Using system environment variables.")
        
        self.environment = self._get_environment()
        self.storage = self.__create_storage()


    def _get_environment(self) -> Environment:
        env_str = self.get_env_var("ENVIRONMENT", "dev").lower()

        env_mapping = {
            "dev": self.Environment.DEV,
            "prod": self.Environment.PROD,
            "test": self.Environment.TESTING
        }

        return env_mapping.get(env_str, self.Environment.DEV)
    
    def get_env_var(self, var_name: str, default: str = None) -> str:
        return os.getenv(var_name, default)
    
    def __create_storage(self):
        """ Returns the configured storage service based on env var """
        storage_provider = self.get_env_var("STORAGE_TYPE", "local").lower()
        if storage_provider == "s3":
            s3_bucket = NBASettings.get_s3_data_bucket()
            return S3Storage(s3_bucket)
        else:
            return LocalStorage(base_directory="data")
        
class NBASettings:
    """
    NBA-specific settings
    """
    # For now this is hardcoded, but could be made dynamic in the future
    DEFAULT_SEASON: str = ''

    # Same here
    DEFAULT_SEASONS_LIST: List[str] = []

    def __init__(self):
        self.DEFAULT_SEASON = self.get_current_season()
        self.DEFAULT_SEASONS_LIST = self.get_season_list(num_seasons=3)

    @staticmethod
    def get_current_season() -> str:
        """ Get the current NBA season in format '2023-24' """
        today = date.today()
        year = today.year
        month = today.month


        # NBA season starts in October
        if month >= 10:
            start_year = year
            end_year = year + 1
        else:
            start_year = year - 1
            end_year = year
        
        return f"{start_year}-{str(end_year)[-2:]}"

    @staticmethod
    def get_season_list(num_seasons: int = 5) -> List[str]:
        """
        Return a list of the most recent `num_years` seasons including current.
        """
        current_season = NBASettings.get_current_season()
        start_year = int(current_season.split('-')[0])

        seasons = []
        for i in range(num_seasons):
            season_start = start_year - i
            season_end = season_start + 1
            seasons.append(f"{season_start}-{str(season_end)[-2:]}")

        return seasons

    @staticmethod
    def get_s3_data_bucket() -> str:
        """ Get S3 bucket name based on environment """
        bucket_name = settings.get_env_var("S3_NBA_DATA_BUCKET_NAME", "nba-analytics-data")
        return f"{bucket_name}-{settings.environment.value}"

settings = Settings()
nba_settings = NBASettings()
