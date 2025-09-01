import os
from enum import Enum
from pathlib import Path
from dotenv import load_dotenv

from ..services.storage.s3_storage import S3Storage
from ..services.storage.local_storage import LocalStorage
from ..services.nba.nba_settings import NBASettings

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
    
    def get_storage_service(self):
        """ Returns the configured storage service based on env var """
        storage_provider = self.get_env_var("STORAGE_TYPE", "local").lower()
        if storage_provider == "s3":
            s3_bucket = NBASettings.get_s3_data_bucket()
            return S3Storage(s3_bucket)
        else:
            return LocalStorage(base_directory="data")

settings = Settings()