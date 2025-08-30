from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from ..services.llm.nba_query_processor import NBAQueryProcessor

import logging
logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/health")
def health_check():
    return {"status": "ok"}

class NBAQueryRequest(BaseModel):
    question: str

@router.post("/query")
def process_nba_query(request: NBAQueryRequest):
    try: 
        proccessor = NBAQueryProcessor()
        analyze = proccessor._analyze_query(request.question)
        return {"analysis": analyze}
    except Exception as e:
        logger.error(f"Error in NBA query endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))
