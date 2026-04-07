"""
API Module

This module provides a FastAPI server for querying the RAG system and health checks.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from rag.retriever import RAGRetriever
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="AI Data Platform API", description="API for natural language queries on data platform")

# Initialize RAG Retriever
try:
    retriever = RAGRetriever()
except Exception as e:
    logger.error(f"Failed to initialize RAG Retriever: {e}")
    retriever = None

class QueryRequest(BaseModel):
    """
    Request model for queries.
    """
    question: str

@app.post("/query")
async def query_data(request: QueryRequest):
    """
    Endpoint to query the data platform using natural language.

    Args:
        request (QueryRequest): Query request with question

    Returns:
        dict: Response with answer
    """
    if not retriever:
        raise HTTPException(status_code=500, detail="RAG system not available")

    try:
        answer = retriever.query(request.question)
        logger.info(f"Query processed: {request.question}")
        return {"answer": answer}
    except Exception as e:
        logger.error(f"Error processing query: {e}")
        raise HTTPException(status_code=500, detail="Error processing query")

@app.get("/health")
async def health_check():
    """
    Health check endpoint.

    Returns:
        dict: Health status
    """
    if retriever:
        return {"status": "healthy", "rag_system": "available"}
    else:
        return {"status": "unhealthy", "rag_system": "unavailable"}

if __name__ == "__main__":
    import uvicorn
    host = os.getenv('API_HOST', '0.0.0.0')
    port = int(os.getenv('API_PORT', 8000))
    uvicorn.run(app, host=host, port=port)