"""
Unit Tests

Basic unit tests for the data platform components.
"""

import pytest
from ingestion.producer import generate_transaction
from rag.retriever import RAGRetriever
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_generate_transaction():
    """
    Test transaction generation.
    """
    transaction = generate_transaction()
    assert isinstance(transaction, dict)
    assert 'user_id' in transaction
    assert 'product_id' in transaction
    assert 'category' in transaction
    assert 'price' in transaction
    assert 'quantity' in transaction
    assert 'timestamp' in transaction
    assert transaction['category'] in ['electronics', 'clothing', 'books', 'home', 'sports']
    assert isinstance(transaction['price'], float)
    assert 1 <= transaction['quantity'] <= 5

def test_rag_retriever_initialization():
    """
    Test RAG Retriever initialization (requires FAISS index and OpenAI key).
    """
    # This test assumes the index exists and OPENAI_API_KEY is set
    if os.path.exists(os.getenv('FAISS_INDEX_PATH', './rag/faiss_index')) and os.getenv('OPENAI_API_KEY'):
        retriever = RAGRetriever()
        assert retriever is not None
        # Test a simple query
        answer = retriever.query("What is the total sales?")
        assert isinstance(answer, str)
    else:
        pytest.skip("FAISS index or OpenAI API key not available")

# Add more tests as needed