"""
RAG Retriever Module

This module provides a class to load FAISS index and perform RAG-based queries.
"""

import os
from langchain.vectorstores import FAISS
from langchain.embeddings import OpenAIEmbeddings
from langchain.chains import RetrievalQA
from langchain.llms import OpenAI
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RAGRetriever:
    """
    RAG Retriever class for natural language queries on data.
    """

    def __init__(self, index_path=None, openai_api_key=None):
        """
        Initialize the retriever.

        Args:
            index_path (str): Path to FAISS index
            openai_api_key (str): OpenAI API key
        """
        self.index_path = index_path or os.getenv('FAISS_INDEX_PATH', './rag/faiss_index')
        self.openai_api_key = openai_api_key or os.getenv('OPENAI_API_KEY')

        if not self.openai_api_key:
            raise ValueError("OpenAI API key not provided")

        try:
            self.embeddings = OpenAIEmbeddings(openai_api_key=self.openai_api_key)
            self.vectorstore = FAISS.load_local(self.index_path, self.embeddings)
            self.llm = OpenAI(openai_api_key=self.openai_api_key)
            self.qa_chain = RetrievalQA.from_chain_type(
                llm=self.llm,
                chain_type="stuff",
                retriever=self.vectorstore.as_retriever()
            )
            logger.info("RAG Retriever initialized.")
        except Exception as e:
            logger.error(f"Error initializing retriever: {e}")
            raise

    def query(self, question):
        """
        Perform a natural language query.

        Args:
            question (str): User's question

        Returns:
            str: Answer from RAG system
        """
        try:
            answer = self.qa_chain.run(question)
            return answer
        except Exception as e:
            logger.error(f"Error in query: {e}")
            return "Sorry, I couldn't process your query."

    def update_index(self, texts):
        """
        Update the FAISS index with new texts.

        Args:
            texts (list): List of new text strings
        """
        try:
            new_vectorstore = FAISS.from_texts(texts, self.embeddings)
            self.vectorstore = new_vectorstore
            self.vectorstore.save_local(self.index_path)
            # Reinitialize qa_chain
            self.qa_chain = RetrievalQA.from_chain_type(
                llm=self.llm,
                chain_type="stuff",
                retriever=self.vectorstore.as_retriever()
            )
            logger.info("Index updated.")
        except Exception as e:
            logger.error(f"Error updating index: {e}")
            raise