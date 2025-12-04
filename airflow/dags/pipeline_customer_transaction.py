"""
Customer Transactions Pipeline DAG
===================================

This DAG orchestrates the complete end-to-end data pipeline:
1. Clean and transform raw CSV data
2. Load cleaned data into PostgreSQL
3. Run DBT transformations to build dimensional model (staging, dimensions, facts, aggregates)
"""

import os
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from steps.clean_transactions_pipeline import clean_transactions_pipeline
from steps.load_to_postgres_pipeline import load_to_postgres_pipeline
from steps.dbt_transformations_pipeline import dbt_transformations_pipeline


# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# DAG ID
DAG_ID = os.path.basename(__file__).replace(".py", "")

# DAG definition
dag_definition = {
    'dag_id': DAG_ID,
    'default_args': default_args,
    'description': 'Complete data pipeline: CSV cleaning, PostgreSQL loading, and DBT transformations',
    'schedule_interval': '@daily',
    'catchup': False,
    'tags': ['data-pipeline', 'csv', 'postgres', 'dbt', 'end-to-end'],
}


@dag(**dag_definition)
def clean_customer_transactions():
    """
    Main DAG function that orchestrates the cleaning and loading pipeline.
    """

    group_id = "Clean-Transactions"
    with TaskGroup(group_id=group_id) as clean_transactions_tg:
        group_id = f"{DAG_ID}.{group_id}"
        clean_transactions_pipeline(group_id)

    group_id = "Load-To-Postgres"
    with TaskGroup(group_id=group_id) as load_to_postgres_tg:
        group_id = f"{DAG_ID}.{group_id}"
        load_to_postgres_pipeline(group_id)

    group_id = "DBT-Transformations"
    with TaskGroup(group_id=group_id) as dbt_transformations_tg:
        group_id = f"{DAG_ID}.{group_id}"
        dbt_transformations_pipeline(group_id)

    # Define pipeline dependencies
    clean_transactions_tg >> load_to_postgres_tg >> dbt_transformations_tg

    return dbt_transformations_tg


# Instantiate the DAG
dag_instance = clean_customer_transactions()
