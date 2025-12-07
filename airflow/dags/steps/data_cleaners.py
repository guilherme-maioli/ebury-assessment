"""
Data cleaning functions for customer transactions.
"""

import pandas as pd


def clean_id_with_prefix(value, prefix=None):
    """
    Remove optional prefix from ID field and convert to integer.

    Args:
        value: The value to clean
        prefix: Optional prefix to remove (e.g., 'T', 'P')

    Examples:
        clean_id_with_prefix('T1010', prefix='T') -> 1010
        clean_id_with_prefix('P100', prefix='P') -> 100
        clean_id_with_prefix('1020', prefix='T') -> 1020
    """
    if pd.isna(value):
        return None

    value_str = str(value).strip()

    # Remove prefix if specified and present
    if prefix and value_str.startswith(prefix):
        value_str = value_str[len(prefix):]

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
