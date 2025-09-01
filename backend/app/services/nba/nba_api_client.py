from .nba_data_collector import NBADataCollector
from ..storage_service import StorageService
from .nba_settings import NBASettings

import pandas as pd
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class NBAApiClient:
    """
    High-level client that orchestrates data collection and storage
    """

    def __init__(self):
        self.cached_data = {}

        s3_bucket = NBASettings.get_s3_data_bucket()
        self.storage = StorageService(s3_bucket)

    def collect_and_store_dataset(self, seasons: List[str] = None, source: str = "local", prefix: str = "nba-data") -> bool:
        """ Collect data for specified seasons and store to specified source (local or s3) """
        collector = NBADataCollector()
        dataset = collector.collect_comprehensive_dataset(seasons=seasons)

        if not dataset:
            logger.error("No data collected to store.")
            return False

        if source == "local":
            return self.storage.save_to_local(dataset)
        elif source == "s3":
            return self.storage.save_to_s3(dataset, prefix=prefix)
        else:
            logger.error(f"Unknown storage source: {source}")
            return False