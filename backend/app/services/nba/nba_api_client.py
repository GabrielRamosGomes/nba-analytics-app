from .nba_data_collector import NBADataCollector
from ..storage.base_storage import BaseStorage 

from ...core.settings import NBASettings
from ...core.cache import cache

import pandas as pd
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class NBAApiClient:
    """
    High-level client that orchestrates data collection and storage
    """

    def __init__(self, storage: BaseStorage):
        self.cached_data: Dict[str, pd.DataFrame] = {}
        self.storage = storage

    def collect_and_store_dataset(self, seasons: List[str] = None, prefix: str = "nba-data") -> bool:
        """ Collect data for specified seasons and store to specified source (local or s3) """
        collector = NBADataCollector()
        dataset = collector.collect_dataset(seasons=seasons)

        if not dataset:
            logger.error("No data collected to store.")
            return False

        success = self.storage.save(dataset=dataset, prefix=prefix)
        if success:
            cache.set(f"dataset:{prefix}", dataset)

        return success
 
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
        
    def get_player_stats(self, players: List[str], seasons: List[str]) -> pd.DataFrame:
        """ Get player stats for given players and seasons """
        if not self.cached_data:
            self.load_data()
            
        # Get "player" dataframe that contains all the players stats
        player_df = self.cached_data.get("player", pd.DataFrame())
        if player_df.empty:
            logger.warning("Player dataframe is empty.")
            return pd.DataFrame()
        
        name_col = "PLAYER_NAME"
        season_col = "season"

        if "PLAYER_NAME" not in player_df.columns:
            logger.warning('Expected column "PLAYER_NAME" not found in player dataframe.')
            return pd.DataFrame()
        if "season" not in player_df.columns:
            logger.warning('Expected column "season" not found in player dataframe.')
            return pd.DataFrame()
        
        filtered = player_df[
            player_df[name_col].str.lower().isin([p.lower() for p in players]) &
            player_df[season_col].isin(seasons)
        ]
        logger.info(f"Filtered player stats: {len(filtered)} records found for players {players} in seasons {seasons}")
        return filtered


    def load_data(self, prefix: str = "nba-data", latest_only: bool = True) -> Dict[str, pd.DataFrame]:
        """ Load data using the configured storage """
        try:
            cache_key = f"dataset:{prefix}:latest" if latest_only else f"dataset:{prefix}:all"
            cached = cache.get(cache_key)

            if cached:
                logger.info(f"Loaded dataset from cache with key={cache_key}")
                self.cached_data = cached
                return cached
            
            dataset = self.storage.load(prefix=prefix, latest_only=latest_only)

            cache.set(cache_key, dataset)
            self.cached_data = dataset
            if not dataset:
                logger.warning("No data loaded from storage.")

            return dataset
        except Exception as e:
            logger.error(f"Failed to load data from storage: {e}")
            return {}