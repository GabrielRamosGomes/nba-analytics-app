import pandas as pd
import json
import logging
from typing import Dict, Any, Generator, List, Optional
from langchain.schema import HumanMessage, SystemMessage
from pydantic import BaseModel, Field
from enum import Enum

from ..nba.nba_api_client import NBAApiClient
from .llm_factory import get_llm
from ..storage.base_storage import BaseStorage
from ...core.settings import nba_settings


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

class QueryAnalysis(BaseModel):
    """Structured output for NBA query analysis"""
    intent: QueryIntent = Field(description="The type of NBA query being asked")
    players: List[str] = Field(default=[], description="List of player names mentioned")
    teams: List[str] = Field(default=[], description="List of team names mentioned")
    seasons: List[str] = Field(default=[], description="List of seasons in format '2023-24'")
    stats: List[str] = Field(default=[], description="Statistical categories like 'points', 'assists', 'rebounds'")
    stats_type: Optional[str] = Field(default=None, description="Type of stats: per_game, totals, advanced")
    timeframe: Timeframe = Field(default=Timeframe.SEASON, description="Time scope of the query")
    comparison_type: Optional[ComparisonType] = Field(default=None, description="Type of comparison if applicable")
    top_n: int = Field(default=10, description="Number of top performers if applicable")

class QueryProcessor:
    """
    Processes natural language queries about NBA data using LangChain
    """

    def __init__(self, storage: BaseStorage):
        self.llm = get_llm()
        self.nba_client = NBAApiClient(storage=storage)

    def query(self, query: str):
        analysis = self._analyze_query(query)
        
        data = self._fetch_relevant_data(analysis)
        
        answer = self._generate_answer(original_question=query, analysis=analysis, data=data)

        return answer

    def _analyze_query(self, question: str) -> Dict[str, Any]:
        """
        Analyze the user's question to extract intent and parameters
        """

        structured_llm = self.llm.with_structured_output(QueryAnalysis)

        system_prompt = f"""
            You are an NBA data analyst. Analyze the user's question and extract the following information in JSON format:

            {{
                "intent": "one of: player_stats, player_comparison, team_stats, team_comparison, top_performers, season_analysis",
                "players": ["list of player names mentioned"],
                "teams": ["list of team names mentioned"], 
                "seasons": ["list of seasons mentioned, convert to format like '2023-24'"],
                "stats": ["list of statistical categories mentioned like 'points', 'assists', 'rebounds'"],
                "stats_type": "one of: per_game, totals, advanced". If not specified default to per_game",
                "timeframe": "one of: season, career, game, recent",
                "comparison_type": "if comparing, what type: vs, ranking, top_n",
                "top_n": "if asking for top performers, how many (default 10)"
            }}

            Available seasons: {', '.join(nba_settings.DEFAULT_SEASONS_LIST)}
            If no season is specified, assume current season ({nba_settings.DEFAULT_SEASON}).
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
                result["seasons"] = [nba_settings.DEFAULT_SEASON]

            logger.info(f"Query analysis successful: {result}")
            return result
        except json.JSONDecodeError:
            logger.error("Failed to parse LLM output, returning default analysis")
            return {
                "intent": "general",
                "players": [],
                "teams": [],
                "seasons": [nba_settings.DEFAULT_SEASON],
                "stats": [],
                "timeframe": "season"
            }
        
    def _fetch_relevant_data(self, analysis: Dict[str, Any]) -> pd.DataFrame:
        """
        Fetch relevant NBA data based on the analysis
        """
        
        intent = analysis.get("intent", "player_stats") # Use player_stats as a fallback
        players =  analysis.get("players", [])
        teams = analysis.get("teams", [])
        seasons = analysis.get("seasons", [nba_settings.DEFAULT_SEASON])
        top_n = analysis.get("top_n", 10)

        try:
            if intent in [QueryIntent.PLAYER_COMPARISON, QueryIntent.PLAYER_STATS]:
                logger.info(f"Fetching player stats for players: {players} in seasons: {seasons}")
                data = self.nba_client.get_player_stats(players=players, seasons=seasons)
            # elif intent in [QueryIntent.TEAM_COMPARISON, QueryIntent.TEAM_STATS]:
            #     logger.info(f"Fetching team stats for teams: {teams} in seasons: {seasons}")
            #     data = self.nba_client.get_team_stats(teams=teams, seasons=seasons)
            # elif intent == QueryIntent.TOP_PERFORMERS:
            #     logger.info(f"Fetching top {top_n} performers in seasons: {seasons}")
            #     data = self.nba_client.get_top_performers(seasons=seasons, top_n=top_n)
            # else:
            #     logger.info(f"Fetching general player stats for seasons: {seasons[0]}")
            #     season = seasons[0] if seasons else nba_settings.DEFAULT_SEASON
            #     data = self.nba_client.get_player_stats(seasons=[season])
            else:
                logger.info(f"Intent '{intent}' not recognized. Returning empty DataFrame.")
                data = pd.DataFrame()

            logger.info(f"Fetched {len(data)} records")
            return data
        except Exception as e:
            logger.error(f"Error fetching NBA data: {e}")
            return pd.DataFrame()
        
    def _generate_answer(self, original_question: str, analysis: Dict[str, Any], data: pd.DataFrame, stream: bool = False) -> Generator[Any, Any, Any]:
        """ Generate a natural language answer based on the analysis and data """
        try:
            if data.empty:
                return "No relevant NBA data found to answer your question."

            intent = analysis.get("intent", "player_stats")
            players =  analysis.get("players", [])
            teams = analysis.get("teams", [])
            seasons = analysis.get("seasons", [nba_settings.DEFAULT_SEASON])
            stats = analysis.get("stats", [])
            stats_type = analysis.get("stats_type", "per_game")
            top_n = analysis.get("top_n", 10)
            timeframe = analysis.get("timeframe", "season")

            system_prompt = f"""
                You are an expert NBA analyst. Use the provided data to answer the user's question.

                User's question intent: {intent}
                Players mentioned: {', '.join(players) if players else 'None'}
                Teams mentioned: {', '.join(teams) if teams else 'None'}
                Timeframe: {timeframe}
                Seasons: {', '.join(seasons)}
                Stats of interest: {', '.join(stats) if stats else 'All available stats'}
                Stats type: {stats_type}
                Top N (if applicable): {top_n}
                
                If the stats type is totals use columns ending with '_TOTALS', if per_game use '_PER_GAME'.
                Data columns available: {', '.join(data.columns.tolist())}

                Provide a concise, informative answer based on the data.
            """
            data_sample = data.head(10).to_dict(orient="records")  # Limit to first 10 records for context

            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=f"Here is some NBA data:\n{json.dumps(data_sample, indent=2)}"),
                HumanMessage(content="Based on this data, please answer the user's question.")
            ]

            response = self.llm.invoke(messages)
            answer = response.content

            return answer
        except Exception as e:
            logger.error(f"Error generating answer: {e}")
            return "Sorry, I encountered an error while generating the answer."
        
        

    
        