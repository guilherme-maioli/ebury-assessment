# Stage 1: Data Ingestion and Preparation

This document describes the first stage of the pipeline: ingesting raw CSV data, cleaning it, and loading it into PostgreSQL.

## Table of Contents

- [Overview](#overview)
- [Data Quality Challenges](#data-quality-challenges)
- [Task 1: Clean and Transform CSV](#task-1-clean-and-transform-csv)
- [Task 2: Load to PostgreSQL](#task-2-load-to-postgresql)
- [Implementation Details](#implementation-details)
- [Performance Considerations](#performance-considerations)

---

## Overview

**Pipeline Stage**: Data Ingestion and Preparation  
**TaskGroups**: `Clean-Transactions` and `Load-To-Postgres` (main dag: `pipeline_customer_transaction`) <br>
**Input**: `data/customer_transactions.csv` (raw data in local directory)  <br>
**Output**: `cleaned_data.customer_transactions` (PostgreSQL table) 

### Goals

1. Clean and validate raw transaction data
2. Standardize data formats and types
3. Load data efficiently into PostgreSQL
4. Ensure data quality before transformation

---

## Data Quality Challenges

The raw `customer_transactions.csv` file contains several data quality issues:

### 1. Mixed Date Formats

```
11-07-2023  (dd-mm-yyyy)
2023-07-11  (yyyy-mm-dd)
```

**Solution**: Detect format and standardize to `yyyy-mm-dd`

### 2. Text Values in Numeric Fields

```csv
price: "Two Hundred"
tax: "Fifteen"
```

**Solution**: Map text to numbers using dictionary lookup

### 3. Prefixed Identifiers

```csv
transaction_id: "T1010"
product_id: "P100"
```

**Solution**: Remove prefixes and convert to integers

### 4. Inconsistent Data Types

All fields stored as VARCHAR/text in source

**Solution**: Cast to appropriate types (INTEGER, DATE, NUMERIC)

### 5. Float Representation

```csv
customer_id: "501.0"
quantity: "2.0"
```

**Solution**: Convert to integers

### 6. NULL and Empty Values

Missing data represented inconsistently

**Solution**: Standardize NULL handling using pandas nullable types

---

## Task 1: Clean and Transform CSV

**TaskGroup**: `Clean-Transactions`
**Task**: `clean_and_transform_csv`
**Module**: `airflow/dags/steps/clean_transactions_pipeline.py`

### Process Flow

```
1. Read CSV file
   ↓
2. Apply cleaning functions to each column
   ↓
3. Cast to appropriate data types
   ↓
4. Handle NULL values
   ↓
5. Write cleaned CSV
   ↓
6. Log data quality metrics (only airflow log)
```

### Cleaning Functions

Located in `airflow/dags/steps/data_cleaners.py`:

#### `clean_transaction_id(value)`

**Examples**:
- `"T1010"` → `1010`
- `"1020"` → `1020`
- `null` → `None`

#### `clean_customer_id(value)`

**Examples**:
- `"501.0"` → `501`
- `"502"` → `502`

#### `clean_transaction_date(value)`

**Examples**:
- `"11-07-2023"` → `"2023-07-11"`
- `"2023-07-11"` → `"2023-07-11"`

#### `clean_product_id(value)`

**Examples**:
- `"P100"` → `100`
- `"101"` → `101`

#### `clean_price(value)` and `clean_tax(value)`

**Examples**:
- `"Two Hundred"` → `200.0`
- `"150.50"` → `150.5`
- `"Fifteen"` → `15.0`

### Data Type Conversion

After cleaning, columns are cast to proper types:

```python
df['transaction_id'] = df['transaction_id'].astype('Int64')
df['customer_id'] = df['customer_id'].astype('Int64')
df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
df['product_id'] = df['product_id'].astype('Int64')
df['quantity'] = df['quantity'].astype('Int64')
df['price'] = df['price'].astype('float64')
df['tax'] = df['tax'].astype('float64')
```

**Note**: Uses pandas nullable integer type (`Int64`) to handle NULLs properly

### Output

Cleaned data is written to `data/customer_transactions_cleaned.csv`:
- Proper data types
- Standardized formats
- NULL values represented as empty strings
- Ready for database loading

### Data Quality Metrics

The task logs comprehensive metrics:

Output of example dataset:
```
Total rows: 100
Null values per column:
transaction_id     0
customer_id        2
transaction_date   0
product_id         0
product_name       0
quantity           5
price              0
tax                0
```

---

## Task 2: Load to PostgreSQL

**TaskGroup**: `Load-To-Postgres`
**Module**: `airflow/dags/steps/load_to_postgres_pipeline.py`

### Sub-Tasks

The loading process is broken into 4 sequential tasks:

```
create_cleaned_table
        ↓
truncate_cleaned_table
        ↓
load_cleaned_csv_to_postgres
        ↓
validate_postgres_load
```

### 2.1 Create Cleaned Table

**Task**: `create_cleaned_table`

Creates the target schema and table if they don't exist:

```sql
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
```

**Key Points**:
- `cleaned_data` schema for organized data storage
- Proper data types (INTEGER, DATE, NUMERIC)
- `loaded_at` timestamp for audit tracking
- Idempotent: safe to run multiple times (truncate table every time)

### 2.2 Truncate Cleaned Table

**Task**: `truncate_cleaned_table`

Removes existing data before loading:

```sql
TRUNCATE TABLE cleaned_data.customer_transactions;
```

**Why truncate?**
- Ensures idempotency (can rerun pipeline)
- Prevents duplicate data


### 2.3 Load CSV to PostgreSQL

**Task**: `load_cleaned_csv_to_postgres`

Uses PostgreSQL **COPY command** for efficient bulk loading:

```python
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
```

**Why COPY instead of INSERT?**

| Feature | INSERT | COPY |
|---------|--------|------|
| Speed | 1x (baseline) | 10-100x faster |
| Memory | High (loads all data) | Low (streaming) |
| Transactions | One per row | Single transaction |


**NULL Handling**:
- `NULL ''` tells PostgreSQL to treat empty strings as NULL
- Coordinates with pandas `na_rep=''` in CSV writing
- Ensures proper NULL representation in database

### 2.4 Validate Load

**Task**: `validate_postgres_load`

Performs comprehensive validation:

#### Row Count Validation

```sql
SELECT COUNT(*) FROM cleaned_data.customer_transactions
```

Ensures all rows were loaded successfully

#### Sample Data Check

```sql
SELECT * FROM cleaned_data.customer_transactions LIMIT 5
```

Logs sample data to verify correct loading

#### Data Quality Checks

```sql
SELECT
    COUNT(*) as total_rows,
    COUNT(CASE WHEN transaction_id IS NULL THEN 1 END) as null_transaction_ids,
    COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as null_customer_ids,
    COUNT(CASE WHEN transaction_date IS NULL THEN 1 END) as null_dates,
    COUNT(CASE WHEN quantity IS NULL THEN 1 END) as null_quantities,
    COUNT(CASE WHEN price IS NULL OR price = 0 THEN 1 END) as zero_prices
FROM cleaned_data.customer_transactions
```

**Validates**:
- No NULL transaction IDs
- Expected number of NULL customer IDs
- No NULL dates
- Expected NULL quantities
- No zero or NULL prices

---

## Implementation Details

### Module Pattern

Each pipeline stage follows this pattern:

```python
def pipeline_module(parent_group):
    """
    Pipeline module with encapsulated logic.

    Args:
        parent_group: Task group ID for logging

    Returns:
        Final task in the pipeline
    """

    @task(task_id="task_name")
    def task_function(upstream_output=None):
        # Task logic here
        return result

    # Create tasks with dependencies
    task1 = task_function()
    task2 = another_task(task1)  # Implicit dependency

    return task2
```

**Benefits**:
- Reusable across DAGs
- Testable independently
- Clear dependency chain
- Easy to maintain

### Dependency Management

Tasks use **XCom argument passing** for dependencies:

```python
# Task accepts upstream output
@task(task_id="truncate_cleaned_table")
def truncate_cleaned_table(upstream_output):
    # upstream_output contains result from previous task
    # Dependency is implicit
    pass

# Create dependency by passing output
create_task = create_cleaned_table()
truncate_task = truncate_cleaned_table(create_task)
```

This approach works seamlessly with the `@dag` decorator

---

## Performance Considerations

### Memory Efficiency

- **Pandas**: Efficient in-memory data processing
- **Streaming COPY**: No need to load entire file into memory
- **Single pass transformations**: Process data once

---

## Troubleshooting

### Common Issues

**Issue**: CSV file not found

```bash
# Check file exists
ls -la data/customer_transactions.csv

# Check Docker volume mount
docker-compose exec airflow-webserver ls -la /opt/data/
```

**Issue**: Type conversion errors

```bash
# Check data types in cleaned CSV
head data/customer_transactions_cleaned.csv

# View detailed logs
make logs-airflow
```

**Issue**: Duplicate data

```bash
# Verify truncate ran
# Check task logs for truncate_cleaned_table

# Manually truncate if needed
make shell-postgres
TRUNCATE TABLE cleaned_data.customer_transactions;
```

---

## Next Steps

After successful data ingestion and preparation:

1. ✅ Data is cleaned and validated
2. ✅ Data is loaded into PostgreSQL
3. → Ready for **Stage 2: Data Transformation and Aggregation** (DBT models)

See [Stage 2 Documentation](02-data-transformation-aggregation.md) for details on DBT transformations.

---

## Summary

**Stage 1** accomplishes:

✅ Cleans raw CSV data (format standardization, type conversion) <br>
✅ Validates data quality (NULL handling, error detection) <br>
✅ Loads data efficiently (PostgreSQL COPY command) <br>
✅ Ensures data integrity (validation checks, error handling) <br>
✅ Provides observability (logging, metrics, samples) <br>

The cleaned data in `cleaned_data.customer_transactions` is now ready for transformation into a dimensional model in Stage 2.
