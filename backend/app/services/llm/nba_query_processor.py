import pandas as pd
import json
import logging
from typing import Dict, Any, List
from langchain.schema import HumanMessage, SystemMessage

from ..nba.nba_api_client import NBAApiClient
from ..nba.nba_settings import NBASettings
from ..llm.llm_factory import get_llm
from ...core.settings import settings


logger = logging.getLogger(__name__)

class NBAQueryProcessor:
    """
    Processes natural language queries about NBA data using LangChain
    """

    def __init__(self):
        self.llm = get_llm()
        self.nba_client = NBAApiClient()

    def _analyze_query(self, question: str) -> Dict[str, Any]:
        """
        Analyze the user's question to extract intent and parameters
        """

        system_prompt = f"""
            You are an NBA data analyst. Analyze the user's question and extract the following information in JSON format:

            {{
                "intent": "one of: player_stats, player_comparison, team_stats, team_comparison, top_performers, season_analysis",
                "players": ["list of player names mentioned"],
                "teams": ["list of team names mentioned"], 
                "seasons": ["list of seasons mentioned, convert to format like '2023-24'"],
                "stats": ["list of statistical categories mentioned like 'points', 'assists', 'rebounds'"],
                "timeframe": "one of: season, career, game, recent",
                "comparison_type": "if comparing, what type: vs, ranking, top_n",
                "top_n": "if asking for top performers, how many (default 10)"
            }}

            Available seasons: {', '.join(NBASettings.DEFAULT_SEASONS_LIST)}
            If no season is specified, assume current season ({NBASettings.DEFAULT_SEASON}).
            Be flexible with player names (LeBron = LeBron James, Curry = Stephen Curry, etc.).
        """

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Analyze this NBA question: {question}")
        ]

        response = self.llm(messages)
        try:
            analysis = json.loads(response.content)

            if not analysis.get("seasons"):
                analysis["seasons"] = [NBASettings.DEFAULT_SEASON]

            return analysis
        except json.JSONDecodeError:
            logger.warning(f"Could not parse LLM analysis: {response.content}")
            return {
                "intent": "general",
                "players": [],
                "teams": [],
                "seasons": [NBASettings.DEFAULT_SEASON],
                "stats": [],
                "timeframe": "season"
            }