import pandas as pd
import json
import logging
from typing import Dict, Any, List, Optional
from langchain.schema import HumanMessage, SystemMessage
from pydantic import BaseModel, Field
from enum import Enum

from ..nba.nba_api_client import NBAApiClient
from ..nba.nba_settings import NBASettings
from ..llm.llm_factory import get_llm
from ...core.settings import settings


logger = logging.getLogger(__name__)

class QueryIntent(str, Enum):
    PLAYER_STATS = "player_stats"
    PLAYER_COMPARISON = "player_comparison"
    TEAM_STATS = "team_stats"
    TEAM_COMPARISON = "team_comparison"
    TOP_PERFORMERS = "top_performers"
    SEASON_ANALYSIS = "season_analysis"

class Timeframe(str, Enum):
    SEASON = "season"
    CAREER = "career"
    GAME = "game"
    RECENT = "recent"

class ComparisonType(str, Enum):
    VS = "vs"
    RANKING = "ranking"
    TOP_N = "top_n"

class NBAQueryAnalysis(BaseModel):
    """Structured output for NBA query analysis"""
    intent: QueryIntent = Field(description="The type of NBA query being asked")
    players: List[str] = Field(default=[], description="List of player names mentioned")
    teams: List[str] = Field(default=[], description="List of team names mentioned")
    seasons: List[str] = Field(default=[], description="List of seasons in format '2023-24'")
    stats: List[str] = Field(default=[], description="Statistical categories like 'points', 'assists', 'rebounds'")
    timeframe: Timeframe = Field(default=Timeframe.SEASON, description="Time scope of the query")
    comparison_type: Optional[ComparisonType] = Field(default=None, description="Type of comparison if applicable")
    top_n: int = Field(default=10, description="Number of top performers if applicable")

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

        structured_llm = self.llm.with_structured_output(NBAQueryAnalysis)

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

        try:
            analysis = structured_llm.invoke(messages)

            result = analysis.model_dump()

            if not result.get("seasons"):
                result["seasons"] = [NBASettings.DEFAULT_SEASON]

            logger.info(f"Query analysis successful: {result}")
            return result
        except json.JSONDecodeError:
            logger.error("Failed to parse LLM output, returning default analysis")
            return {
                "intent": "general",
                "players": [],
                "teams": [],
                "seasons": [NBASettings.DEFAULT_SEASON],
                "stats": [],
                "timeframe": "season"
            }