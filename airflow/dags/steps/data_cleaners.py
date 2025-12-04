"""
Data cleaning functions for customer transactions.
"""

import pandas as pd


def clean_transaction_id(value):
    """
    Remove 'T' prefix from transaction_id and convert to integer.
    Example: 'T1010' -> 1010
    """
    if pd.isna(value):
        return None

    value_str = str(value).strip()
    if value_str.startswith('T'):
        value_str = value_str[1:]

    try:
        return int(value_str)
    except (ValueError, TypeError):
        return None


def clean_customer_id(value):
    """
    Convert customer_id from float to int when not null.
    Example: 501.0 -> 501
    """
    if pd.isna(value):
        return None

    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def clean_transaction_date(value):
    """
    Transform date from dd-mm-yyyy to yyyy-mm-dd format.
    Handle both formats: '2023-07-11' and '11-07-2023'
    """
    if pd.isna(value):
        return None

    value_str = str(value).strip()

    if '-' in value_str:
        parts = value_str.split('-')
        if len(parts) == 3:
            if len(parts[0]) <= 2 and int(parts[0]) <= 31:
                day, month, year = parts
                return f"{year}-{month}-{day}"
            else:
                return value_str

    return value_str


def clean_product_id(value):
    """
    Remove 'P' prefix from product_id and convert to integer.
    Example: 'P100' -> 100
    """
    if pd.isna(value):
        return None

    value_str = str(value).strip()
    if value_str.startswith('P'):
        value_str = value_str[1:]

    try:
        return int(value_str)
    except (ValueError, TypeError):
        return None


def clean_price(value):
    """
    Replace 'Two Hundred' with 200 and convert to float.
    """
    if pd.isna(value):
        return None

    value_str = str(value).strip()

    if value_str.lower() == 'two hundred':
        return 200.0

    try:
        return float(value_str)
    except (ValueError, TypeError):
        return None


def clean_tax(value):
    """
    Replace 'Fifteen' with 15 and convert to float.
    """
    if pd.isna(value):
        return None

    value_str = str(value).strip()

    if value_str.lower() == 'fifteen':
        return 15.0

    try:
        return float(value_str)
    except (ValueError, TypeError):
        return None
