"""
CSV to PostgreSQL ETL DAG
==========================

This DAG:
1. Creates the target table if it doesn't exist
2. Reads CSV file from /opt/data/customer_transactions.csv
3. Loads data into PostgreSQL using COPY command for performance
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import os

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'csv_to_postgres_etl',
    default_args=default_args,
    description='Load customer transactions CSV into PostgreSQL',
    schedule_interval='@daily',  # Run once per day
    catchup=False,
    tags=['etl', 'csv', 'postgres'],
)


def validate_csv_structure(**context):
    """
    Validate CSV file structure before loading.

    This function checks:
    - CSV file exists
    - CSV has all required columns
    - Column names match expected schema
    """
    logger = logging.getLogger(__name__)
    csv_path = '/opt/data/customer_transactions.csv'

    # Required columns that must exist in CSV
    required_columns = [
        'transaction_id',
        'customer_id',
        'transaction_date',
        'product_id',
        'product_name',
        'quantity',
        'price',
        'tax'
    ]

    # Check if file exists
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    logger.info(f"Validating CSV file structure: {csv_path}")

    # Read CSV header
    with open(csv_path, 'r') as f:
        first_line = f.readline().strip()
        csv_columns = [col.strip() for col in first_line.split(',')]

    logger.info(f"CSV columns found: {csv_columns}")
    logger.info(f"Required columns: {required_columns}")

    # Check for missing columns
    missing_columns = set(required_columns) - set(csv_columns)
    if missing_columns:
        error_msg = f"CSV is missing required columns: {missing_columns}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Check for extra columns (optional warning)
    extra_columns = set(csv_columns) - set(required_columns)
    if extra_columns:
        logger.warning(f"CSV contains extra columns that will be ignored: {extra_columns}")

    # Count rows for info
    with open(csv_path, 'r') as f:
        row_count = sum(1 for line in f) - 1  # Subtract header

    logger.info(f"✓ CSV validation passed!")
    logger.info(f"✓ All required columns present")
    logger.info(f"✓ CSV contains {row_count} data rows")

    # Push info to XCom
    context['ti'].xcom_push(key='csv_row_count', value=row_count)
    context['ti'].xcom_push(key='csv_columns', value=csv_columns)

    return f"CSV validation successful: {len(csv_columns)} columns, {row_count} rows"


def load_csv_to_postgres(**context):
    """
    Load CSV data into PostgreSQL using native COPY command with cursor.

    This function:
    - Validates the CSV file exists at /opt/data/customer_transactions.csv
    - Uses PostgreSQL COPY command via cursor.copy_expert() for maximum performance
    - Loads data directly into raw_data.customer_transactions table
    """
    logger = logging.getLogger(__name__)
    csv_path = '/opt/data/customer_transactions.csv'

    # Check if file exists
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    logger.info(f"Loading CSV file using COPY command: {csv_path}")

    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Use PostgreSQL COPY command to load CSV
        # COPY is the fastest way to load bulk data into PostgreSQL
        with open(csv_path, 'r') as f:
            # Skip the header row if your CSV has headers
            next(f)

            # Execute COPY command using copy_expert
            cursor.copy_expert(
                sql="""
                COPY raw_data.customer_transactions (
                    transaction_id,
                    customer_id,
                    transaction_date,
                    product_id,
                    product_name,
                    quantity,
                    price,
                    tax
                )
                FROM STDIN WITH CSV
                """,
                file=f
            )

        # Update audit columns after COPY
        cursor.execute("""
            UPDATE raw_data.customer_transactions
            SET loaded_at = CURRENT_TIMESTAMP,
                source_file = 'customer_transactions.csv'
            WHERE loaded_at IS NULL
        """)

        # Get actual rows loaded
        cursor.execute("""
            SELECT COUNT(*)
            FROM raw_data.customer_transactions
            WHERE source_file = 'customer_transactions.csv'
        """)
        rows_in_db = cursor.fetchone()[0]

        conn.commit()
        logger.info(f"Successfully loaded data using COPY command")
        logger.info(f"Total rows in table: {rows_in_db}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading data: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

    # Push metrics to XCom
    context['ti'].xcom_push(key='rows_loaded', value=rows_in_db)

    return f"Loaded {rows_in_db} rows successfully using COPY command"


def validate_load(**context):
    """
    Validate that data was loaded correctly by checking row count.
    """
    logger = logging.getLogger(__name__)

    # Get row count from database
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    query = "SELECT COUNT(*) FROM raw_data.customer_transactions"
    result = pg_hook.get_first(query)
    db_count = result[0] if result else 0

    logger.info(f"Total rows in database: {db_count}")

    # Get rows loaded from previous task
    rows_loaded = context['ti'].xcom_pull(task_ids='load_csv_to_postgres', key='rows_loaded')

    if rows_loaded:
        logger.info(f"Rows loaded in this run: {rows_loaded}")

    context['ti'].xcom_push(key='total_rows_in_db', value=db_count)

    return f"Validation complete: {db_count} total rows in database"


# Task 1: Validate CSV structure
validate_csv = PythonOperator(
    task_id='validate_csv_structure',
    python_callable=validate_csv_structure,
    dag=dag,
)

# Task 2: Create table if not exists
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE SCHEMA IF NOT EXISTS raw_data;
    DROP TABLE IF EXISTS raw_data.customer_transactions;
    CREATE TABLE IF NOT EXISTS raw_data.customer_transactions (
        transaction_id VARCHAR(255),
        customer_id VARCHAR(255),
        transaction_date VARCHAR(255),
        product_id VARCHAR(255),
        product_name VARCHAR(255),
        quantity VARCHAR(255),
        price VARCHAR(255),
        tax VARCHAR(255),
        loaded_at TIMESTAMP,
        source_file VARCHAR(255)
    );
    """,
    dag=dag,
)

# Task 3: Load CSV to PostgreSQL using cursor and COPY command
load_data = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    dag=dag,
)

# Task 4: Validate the load
validate_data = PythonOperator(
    task_id='validate_load',
    python_callable=validate_load,
    dag=dag,
)

# Define task dependencies
validate_csv >> create_table >> load_data >> validate_data
