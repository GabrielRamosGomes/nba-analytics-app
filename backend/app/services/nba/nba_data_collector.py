import pandas as pd
from datetime import date
from typing import List, Dict, Optional
from nba_api.stats.endpoints import (
    commonplayerinfo,
    playercareerstats,
    leaguedashplayerstats,
    leaguedashteamstats,
    playerindex,
)
from .nba_settings import NBASettings

from nba_api.stats.static import players, teams
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NBADataCollector:
    """
    A class to collect NBA data from various endpoints.
    """

    def get_all_players(self) -> pd.DataFrame:
        """ Get a list of all NBA players. """
        try:
            all_players = players.get_players()
            logger.info("Successfully retrieved all NBA players.")
            return pd.DataFrame(all_players)
        except Exception as e:
            logger.error(f"Error retrieving NBA players: {e}")
            return pd.DataFrame()

    def get_all_teams(self) -> pd.DataFrame:
        """ Get a list of all NBA teams. """
        try:
            all_teams = teams.get_teams()
            logger.info("Successfully retrieved all NBA teams.")
            return pd.DataFrame(all_teams)
        except Exception as e:
            logger.error(f"Error retrieving NBA teams: {e}")
            return pd.DataFrame()
        
    def get_season_player_stats(self, season: str = None) -> pd.DataFrame:
        """
        Get comprehensive player stats for a given season \n
        Season format: "2023-24"
        """
        try:
            logger.info(f"Fetching player stats for season {season}")
            season = season or NBASettings.DEFAULT_SEASON
        
            player_stats = leaguedashplayerstats.LeagueDashPlayerStats(
                season=season,
                season_type_all_star="Regular Season",
            )

            df = player_stats.get_data_frames()[0]
            df["season"] = season
            return df
        except Exception as e:
            logger.error(f"Error retrieving player stats for season {season}: {e}")
            return pd.DataFrame()
        
    def get_team_stats(self, season: str = None) -> pd.DataFrame:
        """
        Get comprehensive team stats for a given season \n
        Season format: "2023-24"
        """
        try:
            season = season or NBASettings.DEFAULT_SEASON
            logger.info(f"Fetching team stats for season {season}")

            team_stats = leaguedashteamstats.LeagueDashTeamStats(
                season=season,
                season_type_all_star="Regular Season",
            )

            df = team_stats.get_data_frames()[0]
            df["season"] = season
            return df
        except Exception as e:
            logger.error(f"Error retrieving team stats for season {season}: {e}")
            return pd.DataFrame()
        
    def get_player_career_stats(self, player_id: int) -> pd.DataFrame:
        """Get career stats for a specific player"""

        try: 
            career_stats = playercareerstats.PlayerCareerStats(player_id=player_id)
            df = career_stats.get_data_frames()[0]
            return df
        except Exception as e:
            logger.error(f"Error retrieving career stats for player {player_id}: {e}")
            return pd.DataFrame()
    
    def collect_comprehensive_dataset(self, seasons: List[str] = None) -> Dict[str, pd.DataFrame]:
        """
        Collect comprehensive NBA dataset for analysis
        """

        if seasons is None:
            seasons = NBASettings.DEFAULT_SEASONS_LIST

        dataset = {}

        # Get Static Data
        logger.info("Collecting players and teams data...")
        dataset['players'] = self.get_all_players()
        dataset['teams'] = self.get_all_teams()

        # Get Seasonal data
        all_player_stats = []
        all_team_stats = []

        for season in seasons:
            logger.info(f"Collecting data for season {season}...")

            # Player stats
            player_stats = self.get_season_player_stats(season)
            if not player_stats.empty:
                all_player_stats.append(player_stats)

            # Team stats
            team_stats = self.get_team_stats(season)
            if not team_stats.empty:
                all_team_stats.append(team_stats)

            # Just in case there is rate limits
            time.sleep(1)

        # Combine all seasons
        if all_player_stats:
            dataset['player_stats'] = pd.concat(all_player_stats, ignore_index=True)

        if all_team_stats:
            dataset['team_stats'] = pd.concat(all_team_stats, ignore_index=True)

        return dataset
    
    def get_player_season_comparison_data(self, player_names: List[str], seasons: List[str] = None) -> pd.DataFrame:
        """
        Get stats for specific players for comparison
        """

        if seasons is None:
            seasons = [NBASettings.DEFAULT_SEASON]

        try:
            comparison_data = []

            for season in seasons:
                season_stats = self.get_season_player_stats(season)

                if season_stats.empty:
                    return pd.DataFrame()
                
                season_comparison = season_stats[
                    season_stats['PLAYER_NAME'].isin(player_names)
                ]

                if not season_comparison.empty:
                    comparison_data.append(season_comparison)

            if comparison_data:
                return pd.concat(comparison_data, ignore_index=True)
            else:
                logger.warning("No comparison data found for the given players and seasons.")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error retrieving player comparison data: {e}")
            return pd.DataFrame()