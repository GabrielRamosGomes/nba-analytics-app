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
        self.collector = NBADataCollector()
        self.cached_data = {}

        s3_bucket = NBASettings.get_s3_data_bucket()
        self.storage = StorageService(s3_bucket)

    def get_player_stats(self, season: str = "2023-24", use_cache: bool = True) -> pd.DataFrame:
        """ Get player stats for a given season, with optional caching """
        cache_key = f"player_stats_{season}"

        if use_cache and cache_key in self.cached_data:
            logger.info(f"Returning cached player stats for season {season}")
            return self.cached_data[cache_key]

        stats = self.collector.get_season_player_stats(season)

        if use_cache:
            self.cached_data[cache_key] = stats

        return stats
    
    def search_players(self, query: str, season: str = "2023-24") -> pd.DataFrame:
        """ Search for players by name """
        player_stats = self.get_player_stats(season=season)

        if player_stats.empty:
            logger.warning("Player stats data is empty.")
            return pd.DataFrame()
        
        # Case insensitive search
        mask = player_stats['PLAYER_NAME'].str.contains(query, case=False, na=False)
        return player_stats[mask]
    
    def compare_players(self, player_names: List[str], seasons: List[str] = None) -> pd.DataFrame:
        """ Compare stats for multiple players across specified seasons """
        return self.collector.get_player_season_comparison_data(player_names, seasons)

    def get_top_performers(self, stat_col: str, top_n: int = 10, season: str = None) -> pd.DataFrame:
        """ Get top performers in a specific stat for a given season """
        season = season or NBASettings.DEFAULT_SEASON

        player_stats = self.get_player_stats(season=season)
        if player_stats.empty or stat_col not in player_stats.columns:
            logger.warning(f"Player stats data is empty or stat column '{stat_col}' not found.")
            return pd.DataFrame()

        return player_stats.nlargest(top_n, stat_col)[['PLAYER_NAME', 'TEAM_ABBREVIATION', stat_col]]
    
    def get_team_stats(self, season: str = None) -> pd.DataFrame:
        """ Get team stats for a given season """
        season = season or NBASettings.DEFAULT_SEASON
        return self.collector.get_team_stats(season=season)

    def collect_and_store_dataset(self, seasons: List[str] = None, source: str = "local", prefix: str = "nba-data") -> bool:
        """ Collect data for specified seasons and store to specified source (local or s3) """
        dataset = self.collector.collect_comprehensive_dataset(seasons=seasons)

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
        
    def load_dataset(self, source: str = "local", **kwargs) -> Dict[str, pd.DataFrame]:
        """ Load dataset from specified source (local or s3) """
        if source == "local":
            return self.storage.load_from_local(**kwargs)
        elif source == "s3":
            return self.storage.load_from_s3(**kwargs)
        else:
            logger.error(f"Unknown data source: {source}")
            return {}
