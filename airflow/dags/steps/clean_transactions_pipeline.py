"""
Customer transactions cleaning pipeline.
"""

from pathlib import Path
import pandas as pd
from airflow.decorators import task
from steps.data_cleaners import (
    clean_id_with_prefix,
    clean_customer_id,
    clean_transaction_date,
    clean_price,
    clean_tax,
)


def clean_transactions_pipeline(parent_group):
    """
    Pipeline to clean and transform customer transactions CSV file.

    Args:
        parent_group: The parent task group ID for logging and tracking
    """

    @task(task_id="clean_and_transform_csv")
    def clean_customer_transactions():
        """
        Clean and transform the customer transactions CSV file.
        """
        input_file = Path('/opt/data/customer_transactions.csv')
        output_file = Path('/opt/data/customer_transactions_cleaned.csv')

        print(f"Reading input file: {input_file}")
        df = pd.read_csv(input_file)

        print(f"Original data shape: {df.shape}")
        print(f"Applying transformations...")

        # Apply cleaning functions
        df['transaction_id'] = df['transaction_id'].apply(lambda x: clean_id_with_prefix(x, prefix='T'))
        df['customer_id'] = df['customer_id'].apply(clean_customer_id)
        df['transaction_date'] = df['transaction_date'].apply(clean_transaction_date)
        df['product_id'] = df['product_id'].apply(lambda x: clean_id_with_prefix(x, prefix='P'))
        df['product_name'] = df['product_name'].str.strip()
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        df['price'] = df['price'].apply(clean_price)
        df['tax'] = df['tax'].apply(clean_tax)

        # Cast to appropriate types
        print(f"[{parent_group}] Casting columns to appropriate types...")
        df['transaction_id'] = df['transaction_id'].astype('Int64')
        df['customer_id'] = df['customer_id'].astype('Int64')
        df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
        df['product_id'] = df['product_id'].astype('Int64')
        df['quantity'] = df['quantity'].astype('Int64')
        df['price'] = df['price'].astype('float64')
        df['tax'] = df['tax'].astype('float64')

        # Save cleaned data with proper NULL handling for PostgreSQL
        print(f" Saving cleaned data to: {output_file}")
        
        # Convert pandas NA to empty string, which will be treated as NULL by PostgreSQL
        df.to_csv(output_file, index=False, na_rep='')

        print(f" Cleaned data shape: {df.shape}")
        print(f"\n Data Quality Summary:")
        print(f"Total rows: {len(df)}")
        print(f"Null values per column:\n{df.isnull().sum()}")

        return str(output_file)

    # Execute the task
    clean_task = clean_customer_transactions()

    return clean_task
