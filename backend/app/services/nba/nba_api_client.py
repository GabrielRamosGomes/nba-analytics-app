from .nba_data_collector import NBADataCollector
from ..storage.base_storage import BaseStorage 

from ...core.settings import nba_settings
from ...core.cache import cache

import pandas as pd
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)
class NBAApiClient:
    """
    High-level client that orchestrates data collection and storage
    """

    STAT_MAP = {
        # Common stats (players & teams)
        "minutes": "MIN",
        "points": "PTS",
        "rebounds": "REB",
        "assists": "AST",
        "blocks": "BLK",
        "steals": "STL",
        "turnovers": "TOV",
        "field goals made": "FGM",
        "field goals attempted": "FGA",
        "three pointers made": "FG3M",
        "three pointers attempted": "FG3A",
        "free throws made": "FTM",
        "free throws attempted": "FTA",
        "plus minus": "PLUS_MINUS",
        "wins": "W",
        "losses": "L",
        "win percentage": "W_PCT",
        "games played": "GP",
        "field goal percentage": "FG_PCT",
        "three point percentage": "FG3_PCT",
        "free throw percentage": "FT_PCT",
        
        # Player-specific stats
        "double-doubles": "DD2",
        "triple-doubles": "TD3",
        "fantasy points": "NBA_FANTASY_PTS",
        "WNBA fantasy points": "WNBA_FANTASY_PTS",
        "age": "AGE",
        "nickname": "NICKNAME",
        "team abbreviation": "TEAM_ABBREVIATION",
        "team count": "TEAM_COUNT",
        
        # Team-specific stats
        "personal fouls drawn": "PFD",
        "personal fouls": "PF",
        "offensive rebounds": "OREB",
        "defensive rebounds": "DREB",
    }

    def __init__(self, storage: BaseStorage):
        self.cached_data: Dict[str, pd.DataFrame] = {}
        self.storage = storage

    def __resolve_stat_column(self, stat: str, stats_type: str = "per_game"):
        """ Resolve user-friendly stat name to actual dataframe column name """
        stat = stat.lower().strip()
        column = self.STAT_MAP.get(stat, stat.upper())

        # Columns that do not have _PER_GAME/_TOTAL suffixes
        no_suffix = {
            "W", "L", "W_PCT", "GP", "FG_PCT", "FG3_PCT", "FT_PCT",
            "AGE", "NICKNAME", "TEAM_ABBREVIATION", "TEAM_COUNT",
            "DD2", "TD3"
        }

        if column in no_suffix:
            return column
        
        # Apply stats_type suffix
        if stats_type == "per_game":
            return f"{column}_PER_GAME"
        elif stats_type == "total":
            return f"{column}_TOTAL"
        else:
            logger.warning(f"Unknown stats_type '{stats_type}'. Defaulting to 'per_game'.")
            return f"{column}_PER_GAME"

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

        seasons = seasons or nba_settings.DEFAULT_SEASONS_LIST
        
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
        player_df = self.cached_data.get(nba_settings.PLAYER_STATS_DF, pd.DataFrame())
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
        return filtered

    def get_team_stats(self, teams: List[str], seasons: List[str]) -> pd.DataFrame:
        """ Get team stats for given teams and seasons """
        if not self.cached_data:
            self.load_data()
            
        # Get "team" dataframe that contains all the teams stats
        team_df = self.cached_data.get(nba_settings.TEAM_STATS_DF, pd.DataFrame())
        if team_df.empty:
            logger.warning("Team dataframe is empty.")
            return pd.DataFrame()
        
        name_col = "TEAM_NAME"
        season_col = "season"

        if "TEAM_NAME" not in team_df.columns:
            logger.warning('Expected column "TEAM_NAME" not found in team dataframe.')
            return pd.DataFrame()
        if "season" not in team_df.columns:
            logger.warning('Expected column "season" not found in team dataframe.')
            return pd.DataFrame()
        
        filtered = team_df[
            team_df[name_col].str.lower().isin([t.lower() for t in teams]) &
            team_df[season_col].isin(seasons)
        ]
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
    
    def get_top_performers(self, seasons: List[str], top_n: int = 10, stat: str = "PTS", entity: str = "player", stats_type: str = "per_game") -> pd.DataFrame:
        if entity == "player":
            return self.get_top_players(seasons=seasons, top_n=top_n, stat=stat, stats_type=stats_type)
        elif entity == "team":
            return self.get_top_teams(seasons=seasons, top_n=top_n, stat=stat, stats_type=stats_type)
        else:
            logger.warning(f"Unknown entity type: {entity}. Expected 'player' or 'team'.")
            return pd.DataFrame()

    def get_top_teams(self, seasons: List[str], top_n: int = 10, stat: str = "PTS", stats_type: str="per_game") -> pd.DataFrame:
        """ Get top N teams based on specified stat for given seasons """ 
        if not self.cached_data:
            self.load_data()

        team_df = self.cached_data.get(nba_settings.TEAM_STATS_DF, pd.DataFrame())
        if team_df.empty:
            logger.warning("Team dataframe is empty.")
            return pd.DataFrame()

        stat = self.__resolve_stat_column(stat, stats_type="per_game")
        logger.info(f"Resolved stat column: {stat}")
        if stat not in team_df.columns:
            logger.warning(f"Stat column '{stat}' not found in team dataframe.")
            return pd.DataFrame()
        
        if "season" not in team_df.columns:
            logger.warning('Expected column "season" not found in team dataframe.')
            return pd.DataFrame()
        
        filtered = team_df[team_df["season"].isin(seasons)]
        if filtered.empty:
            logger.warning("No team data found for the specified seasons.")
            return pd.DataFrame()
        
        top_df = (
            filtered[["TEAM_NAME", "season", stat]]
            .sort_values(by=stat, ascending=False)
            .groupby("season")
            .head(top_n)
        )

        return top_df.reset_index(drop=True)

    def get_top_players(self, seasons: List[str], top_n: int = 10, stat: str = "PTS", stats_type="per_game") -> pd.DataFrame:
        """ Get top N players based on specified stat for given seasons """
        pass