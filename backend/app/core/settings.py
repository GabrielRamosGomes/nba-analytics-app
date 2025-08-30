import os
from enum import Enum

class Settings:
    """
    Centralized application settings
    """

    class Environment(Enum):
        DEV = "dev"
        PROD = "prod"
        TESTING = "test"

    def __init__(self):
        self.environment = self._get_environment()

    def _get_environment(self) -> Environment:
        env_str = os.getenv("ENVIRONMENT", "dev").lower()

        env_mapping = {
            "dev": self.Environment.DEV,
            "prod": self.Environment.PROD,
            "test": self.Environment.TESTING
        }

        return env_mapping.get(env_str, self.Environment.DEV)
    

settings = Settings()