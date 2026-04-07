"""
Airflow DAG for Data Pipeline

This DAG orchestrates batch processing and RAG embedding updates.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='Daily data pipeline for batch processing and RAG updates',
    schedule_interval='@daily',  # Run daily
    catchup=False,
)

# Task to run batch processing
batch_process_task = BashOperator(
    task_id='batch_process',
    bash_command='cd /path/to/ai-data-platform && python processing/batch_processor.py',
    dag=dag,
)

# Task to build embeddings
build_embeddings_task = BashOperator(
    task_id='build_embeddings',
    bash_command='cd /path/to/ai-data-platform && python rag/build_embeddings.py',
    dag=dag,
)

# Set task dependencies
batch_process_task >> build_embeddings_task