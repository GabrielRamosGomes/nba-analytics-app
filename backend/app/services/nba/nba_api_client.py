from .nba_data_collector import NBADataCollector
from .nba_settings import NBASettings
from ..storage.base_storage import BaseStorage 

import pandas as pd
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class NBAApiClient:
    """
    High-level client that orchestrates data collection and storage
    """

    def __init__(self, storage: BaseStorage):
        self.cached_data = {}
        self.storage = storage


    def collect_and_store_dataset(self, seasons: List[str] = None, prefix: str = "nba-data") -> bool:
        """ Collect data for specified seasons and store to specified source (local or s3) """
        collector = NBADataCollector()
        dataset = collector.collect_comprehensive_dataset(seasons=seasons)

        if not dataset:
            logger.error("No data collected to store.")
            return False

        return self.storage.save(dataset=dataset, prefix=prefix)
 
    def setup_nba_dataset(self, seasons: List[str] = None, prefix: str = "nba-data") -> bool:
        """ Setup NBA dataset by collecting and storing data  \n
            Source can be 'local' or 's3'  \n
            Seasons is a list of season strings like ["2022-23", "2023-24"]
        """
        logger.info("Starting NBA dataset setup...")

        seasons = seasons or NBASettings.DEFAULT_SEASONS_LIST
        
        try:
            logger.info("Collecting and storing NBA dataset...")
            success = self.collect_and_store_dataset(seasons=seasons, prefix=prefix)

            if not success:
                logger.error("Failed to collect and store NBA dataset.")
                return False

            return True

        except Exception as e:
            logger.error(f"Failed to setup NBA dataset: {e}")
            return False
        
    def load_data(self, prefix: str = "nba-data", latest_only: bool = True) -> Dict[str, pd.DataFrame]:
        """ Load data using the configured storage """
        try:
            dataset = self.storage.load(prefix=prefix, latest_only=latest_only)
            self.cached_data = dataset
            if not dataset:
                logger.warning("No data loaded from storage.")
            return dataset
        except Exception as e:
            logger.error(f"Failed to load data from storage: {e}")
            return {}