"""
Load cleaned transactions to PostgreSQL pipeline.
"""

import logging
from pathlib import Path
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_to_postgres_pipeline(parent_group):
    """
    Pipeline to load cleaned CSV data into PostgreSQL.

    Args:
        parent_group: The parent task group ID for logging and tracking
    """

    @task(task_id="create_cleaned_table")
    def create_cleaned_table():
        """
        Create the cleaned transactions table in PostgreSQL if it doesn't exist.
        """
        logger = logging.getLogger(__name__)
        logger.info(f"[{parent_group}] Creating cleaned transactions table")

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        create_table_sql = """
        CREATE SCHEMA IF NOT EXISTS cleaned_data;

        CREATE TABLE IF NOT EXISTS cleaned_data.customer_transactions (
            transaction_id INTEGER,
            customer_id INTEGER,
            transaction_date DATE,
            product_id INTEGER,
            product_name VARCHAR(255),
            quantity INTEGER,
            price NUMERIC(10,2),
            tax NUMERIC(10,2),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        pg_hook.run(create_table_sql)
        logger.info(f"[{parent_group}] Table created successfully")

        return "Table created"

    @task(task_id="truncate_cleaned_table")
    def truncate_cleaned_table(upstream_output):
        """
        Truncate the cleaned transactions table before loading new data.
        """
        logger = logging.getLogger(__name__)
        logger.info(f"[{parent_group}] Truncating cleaned transactions table")

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        pg_hook.run("TRUNCATE TABLE cleaned_data.customer_transactions;")

        logger.info(f"[{parent_group}] Table truncated successfully")

        return "Table truncated"

    @task(task_id="load_cleaned_csv_to_postgres")
    def load_cleaned_csv_to_postgres(upstream_output):
        """
        Load cleaned CSV data into PostgreSQL using COPY command.
        """
        logger = logging.getLogger(__name__)
        csv_path = '/opt/data/customer_transactions_cleaned.csv'

        # Check if file exists
        if not Path(csv_path).exists():
            raise FileNotFoundError(f"[{parent_group}] Cleaned CSV file not found: {csv_path}")

        logger.info(f"[{parent_group}] Loading cleaned CSV file: {csv_path}")

        # Get PostgreSQL connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            # Use PostgreSQL COPY command to load CSV
            with open(csv_path, 'r') as f:
                # Skip the header row
                next(f)

                # Execute COPY command using copy_expert
                # NULL '' tells PostgreSQL to treat empty strings as NULL
                cursor.copy_expert(
                    sql="""
                    COPY cleaned_data.customer_transactions (
                        transaction_id,
                        customer_id,
                        transaction_date,
                        product_id,
                        product_name,
                        quantity,
                        price,
                        tax
                    )
                    FROM STDIN WITH (FORMAT CSV, NULL '')
                    """,
                    file=f
                )

            # Get rows loaded
            cursor.execute("SELECT COUNT(*) FROM cleaned_data.customer_transactions")
            rows_loaded = cursor.fetchone()[0]

            conn.commit()
            logger.info(f"[{parent_group}] Successfully loaded {rows_loaded} rows using COPY command")

        except Exception as e:
            conn.rollback()
            logger.error(f"[{parent_group}] Error loading data: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

        return f"Loaded {rows_loaded} rows"

    @task(task_id="validate_postgres_load")
    def validate_postgres_load(upstream_output):
        """
        Validate that data was loaded correctly into PostgreSQL.
        """
        logger = logging.getLogger(__name__)

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        # Get row count
        result = pg_hook.get_first("SELECT COUNT(*) FROM cleaned_data.customer_transactions")
        db_count = result[0] if result else 0

        logger.info(f"[{parent_group}] Total rows in cleaned table: {db_count}")

        # Get sample data
        sample_query = """
        SELECT
            transaction_id,
            customer_id,
            transaction_date,
            product_id,
            product_name,
            quantity,
            price,
            tax
        FROM cleaned_data.customer_transactions
        LIMIT 5
        """

        sample_data = pg_hook.get_records(sample_query)
        logger.info(f"[{parent_group}] Sample data (first 5 rows):")
        for row in sample_data:
            logger.info(f"  {row}")

        # Validate data quality
        quality_query = """
        SELECT
            COUNT(*) as total_rows,
            COUNT(CASE WHEN transaction_id IS NULL THEN 1 END) as null_transaction_ids,
            COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as null_customer_ids,
            COUNT(CASE WHEN transaction_date IS NULL THEN 1 END) as null_dates,
            COUNT(CASE WHEN quantity IS NULL THEN 1 END) as null_quantities,
            COUNT(CASE WHEN price IS NULL OR price = 0 THEN 1 END) as zero_prices
        FROM cleaned_data.customer_transactions
        """

        quality_result = pg_hook.get_first(quality_query)
        logger.info(f"[{parent_group}] Data Quality Check:")
        logger.info(f"  Total rows: {quality_result[0]}")
        logger.info(f"  Null transaction_ids: {quality_result[1]}")
        logger.info(f"  Null customer_ids: {quality_result[2]}")
        logger.info(f"  Null dates: {quality_result[3]}")
        logger.info(f"  Null quantities: {quality_result[4]}")
        logger.info(f"  Zero prices: {quality_result[5]}")

        if db_count == 0:
            raise ValueError(f"[{parent_group}] No data loaded into PostgreSQL!")

        return f"Validation complete: {db_count} rows in database"

    # Create tasks with dependencies through function arguments (XCom passing)
    # This approach works with @dag decorator as dependencies are implicit
    create_table_task = create_cleaned_table()
    truncate_task = truncate_cleaned_table(create_table_task)
    load_task = load_cleaned_csv_to_postgres(truncate_task)
    validate_task = validate_postgres_load(load_task)

    return validate_task
