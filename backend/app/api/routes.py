from typing import List, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..services.llm.query_processor import QueryProcessor
from ..services.nba.nba_api_client import NBAApiClient

from ..core.settings import settings

import logging
logger = logging.getLogger(__name__)

router = APIRouter()
storage = settings.storage

@router.get("/health")
def health_check():
    return {"status": "ok"}

class NBAQueryRequest(BaseModel):
    question: str = Field(..., description="Natural language question about NBA data")

@router.post("/query")
def process_nba_query(request: NBAQueryRequest):
    try: 
        processor = QueryProcessor(storage=storage)
        answer = processor.query(request.question)

        logger.info(answer)
        return {"answer": answer }
    except Exception as e:
        logger.error(f"Error in NBA query endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
class SetupDatasetRequest(BaseModel):
    seasons: Optional[List[str]] = Field(default=None, description="List of seasons like ['2022-23', '2023-24']. If None, uses default seasons.")

# Later this will be setup as a scheduled task or admin-triggered action
@router.post("/setup-dataset")
def setup_nba_dataset(request: SetupDatasetRequest):
    client = NBAApiClient(storage=storage)
    success = client.setup_nba_dataset(seasons=request.seasons)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to setup NBA dataset")

    return {"status": "NBA dataset setup completed successfully"}
