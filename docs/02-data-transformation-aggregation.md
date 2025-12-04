# Stage 2: Data Transformation and Aggregation

This document describes the second stage of the pipeline: transforming cleaned data into a dimensional model using DBT (Data Build Tool).

## Table of Contents

- [Overview](#overview)
- [DBT Project Structure](#dbt-project-structure)
- [Data Model Architecture](#data-model-architecture)
- [Model Details](#model-details)
- [Testing Strategy](#testing-strategy)
- [Running DBT Models](#running-dbt-models)

---

## Overview

**Pipeline Stage**: Data Transformation and Aggregation <br>
**TaskGroup**: `DBT-Transformations` <br>
**Tool**: DBT (Data Build Tool) via Astronomer Cosmos <br>
**Input**: `cleaned_data.customer_transactions` <br>
**Output**: Dimensional model (staging, dimensions, facts, aggregates) <br>

### Goals

1. Transform cleaned data into analytics-ready dimensional model
2. Create reusable dimensions (customers, products)
3. Build transaction fact table with proper foreign keys
4. Generate pre-aggregated metrics for reporting
5. Ensure data quality through comprehensive testing

---

## DBT Project Structure

```
dbt/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Database connection profiles
├── packages.yml             # Dependencies (dbt_utils)
│
├── models/
│   ├── staging/
│   │   ├── stg_customer_transactions.sql
│   │   ├── schema.yml
│   │   └── source.yml
│   │
│   ├── dimensions/
│   │   ├── dim_customers.sql
│   │   ├── dim_products.sql
│   │   └── schema.yml
│   │
│   ├── facts/
│   │   ├── fact_transactions.sql
│   │   └── schema.yml
│   │
│   └── aggregates/
│       ├── agg_monthly_sales.sql
│       └── schema.yml
│
└── dbt_packages/            # Installed packages (auto-generated)
    └── dbt_utils/
```

### Configuration Files

**dbt_project.yml**:
```yaml
name: 'customer_transactions_dw'
version: '1.0.0'
config-version: 2

profile: 'ebury'

model-paths: ["models"]
target-path: "target"

models:
  customer_transactions_dw:
    staging:
      +materialized: view
      +schema: staging
    dimensions:
      +materialized: table
      +schema: dimensions
    facts:
      +materialized: table
      +schema: facts
    aggregates:
      +materialized: view
      +schema: aggregates
```

**packages.yml**:
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

---

## Data Model Architecture

### Star Schema Design

```
                Dimensions
                    ↓
┌──────────────┐         ┌──────────────┐
│dim_customers │         │ dim_products │
└──────┬───────┘         └──────┬───────┘
       │                        │
       └──────────┬─────────────┘
                  │
                  ↓
         ┌─────────────────┐
         │fact_transactions│
         └───────┬─────────┘
                 │
                 ↓
         ┌─────────────────┐
         │agg_monthly_sales│
         └─────────────────┘
```

### Materialization Strategy

| Layer | Materialization | Reason |
|-------|----------------|---------|
| **Staging** | View | Lightweight, always fresh |
| **Dimensions** | Table | Reference data, slow-changing |
| **Facts** | Table | Large volume, query performance |
| **Aggregates** | View | Always up-to-date, built on-demand |

---

## Model Details

### Layer 1: Staging

**Model**: `stg_customer_transactions` <br>
**Source**: `cleaned_data.customer_transactions` <br>
**Materialization**: View <br>

**Purpose**:
- Single source of truth for cleaned transaction data
- Add derived temporal fields
- Add business validation flags
- Prepare data for dimensional modeling

**Key Transformations**:

```sql
-- Temporal dimensions
DATE_TRUNC('month', transaction_date)::DATE AS transaction_month
EXTRACT(YEAR FROM transaction_date)::INTEGER AS transaction_year
EXTRACT(QUARTER FROM transaction_date)::INTEGER AS transaction_quarter

-- Calculated fields
quantity * price AS subtotal
(quantity * price) + tax AS total_amount

-- Business flags
CASE
    WHEN customer_id IS NOT NULL
        AND quantity IS NOT NULL AND quantity > 0
        AND price IS NOT NULL AND price > 0
    THEN TRUE
    ELSE FALSE
END AS is_valid_transaction
```

**Output Schema**:
- All original fields (transaction_id, customer_id, product_id, etc.)
- Temporal dimensions (month, year, quarter, month_name, day_name)
- Calculated fields (subtotal, total_amount)
- Business flags (is_valid_quantity, is_valid_price, is_valid_transaction)
- Audit fields (loaded_at, transformed_at)

---

### Layer 2: Dimensions

#### dim_customers

**Purpose**: Customer dimension <br>
**Grain**: One row per unique customer <br>
**Materialization**: Table <br>

**Business Logic**:

```sql
-- Customer segmentation by transaction volume
CASE
    WHEN transaction_count > 10 THEN 'Gold'
    WHEN transaction_count > 5 THEN 'Silver'
    ELSE 'Bronze'
END AS customer_tier

-- Value segmentation by lifetime value
CASE
    WHEN total_lifetime_value > 10000 THEN 'High Value'
    WHEN total_lifetime_value > 5000 THEN 'Medium Value'
    ELSE 'Low Value'
END AS customer_value_segment
```

**Key Fields**:
- `customer_key` 
- `customer_id` 
- `transaction_count` (Lifetime transactions)
- `total_lifetime_value` (Sum of all purchases)
- `customer_tier` (Gold/Silver/Bronze)
- `customer_value_segment` (High/Medium/Low)
- `first_transaction_date`, `last_transaction_date`

#### dim_products

**Purpose**: Product dimension <br>
**Grain**: One row per unique product <br>
**Materialization**: Table <br>

**Business Logic**:

```sql
-- Price tier classification
CASE
    WHEN avg_price > 1000 THEN 'Premium'
    WHEN avg_price > 100 THEN 'Mid-Range'
    ELSE 'Budget'
END AS price_tier

-- Revenue segment by total revenue
CASE
    WHEN total_revenue > 50000 THEN 'Top Seller'
    WHEN total_revenue > 10000 THEN 'Good Seller'
    ELSE 'Regular Seller'
END AS revenue_segment
```

**Key Fields**:
- `product_key` 
- `product_id` 
- `product_name`
- `times_sold` (Total orders)
- `total_quantity_sold`
- `avg_price`, `min_price`, `max_price`
- `price_tier` (Premium/Mid-Range/Value)
- `revenue_segment`

---

### Layer 3: Facts

**Model**: `fact_transactions` <br>
**Purpose**: Transaction fact table <br>
**Grain**: One row per transaction <br>
**Materialization**: Table

**Structure**:

```sql

transaction_key 

-- Foreign keys
customer_fk → dim_customers.customer_key
product_fk → dim_products.product_key

-- Temporal dimensions
transaction_date
transaction_month
transaction_year
transaction_quarter

-- Measures 
units_sold
gross_amount (price * quantity)
tax_amount
net_amount (gross + tax)
revenue_excluding_tax
unit_price 

-- Denormalized dimensions (for query performance)
customer_tier
customer_value_segment
product_name
product_price_tier
product_revenue_segment
```

**Why Denormalize**:
- Faster queries (avoid joins for common filters)
- Snapshot of dimension values at transaction time

---

### Layer 4: Aggregates

**Model**: `agg_monthly_sales` <br>
**Purpose**: Pre-aggregated monthly sales metrics <br>
**Grain**: One row per customer per month <br>
**Materialization**: View (always up-to-date) <br>

**Key Metrics**:

```sql
-- Transaction metrics
COUNT(*) AS transaction_count
COUNT(DISTINCT product_fk) AS unique_products_purchased

-- Volume metrics
SUM(units_sold) AS total_units
AVG(units_sold) AS avg_units_per_transaction

-- Revenue metrics
SUM(gross_amount) AS monthly_gross_revenue
SUM(tax_amount) AS monthly_tax
SUM(net_amount) AS monthly_net_revenue
AVG(net_amount / transaction_count) AS avg_revenue_per_transaction

-- Price analytics
AVG(unit_price) AS avg_unit_price
MIN(unit_price) AS min_unit_price
MAX(unit_price) AS max_unit_price

-- Growth metrics 
LAG(monthly_net_revenue) OVER (
    PARTITION BY customer_fk
    ORDER BY transaction_month
) AS previous_month_revenue

CASE
    WHEN previous_month_revenue > 0
    THEN ((monthly_net_revenue - previous_month_revenue) / previous_month_revenue * 100)
    ELSE NULL
END AS revenue_growth_pct
```

**Use Cases**:
- Monthly revenue dashboards
- Customer trend analysis
- Growth rate reporting
- Sales forecasting

---

## Testing Strategy

DBT includes **tests** across all models to ensure data quality:

### Test Categories

#### 1. Uniqueness Tests

```yaml
- name: customer_key
  tests:
    - unique
    - not_null
```

Ensures primary keys are unique and not null

#### 2. Relationship Tests

```yaml
- name: customer_fk
  tests:
    - relationships:
        to: ref('dim_customers')
        field: customer_key
```

Validates foreign key integrity

#### 3. Accepted Values Tests

```yaml
- name: customer_tier
  tests:
    - accepted_values:
        values: ['Platinum', 'Gold', 'Silver', 'Bronze']
```

Ensures categorical fields have expected values

#### 4. Range Tests (dbt_utils)

```yaml
- name: transaction_year
  tests:
    - dbt_utils.accepted_range:
        min_value: 2020
        max_value: 2030
```

Validates numeric ranges

#### 5. Expression Tests (dbt_utils)

```yaml
tests:
  - dbt_utils.expression_is_true:
      expression: "max_unit_price >= min_unit_price"
```

Custom business logic validation

#### 6. Composite Key Tests (dbt_utils)

```yaml
tests:
  - dbt_utils.unique_combination_of_columns:
      combination_of_columns:
        - customer_fk
        - transaction_month
```

Validates composite uniqueness

### Test Execution

Tests run automatically after each model build:

```
dbt run    # Build models
    ↓
dbt test   # Run tests (automatic with Cosmos)
```

---

## Running DBT Models

### Via Airflow (Automated)

DBT models run automatically as part of the `pipeline_customer_transaction` DAG:


### Via Makefile (Manual)

```bash
# Install dependencies
make dbt-deps

# Run all models
make dbt-run

# Run tests
make dbt-test

# Compile models (check SQL)
make dbt-compile
```

### Via DBT CLI

```bash
# Enter Airflow container
make shell-airflow

# Navigate to DBT project
cd /opt/dbt

# Run commands
dbt run --profiles-dir .
dbt test --profiles-dir .
dbt run --select +fact_transactions  # Run model and upstream dependencies
dbt run --select fact_transactions+  # Run model and downstream dependencies
```

---

## Model Dependencies

DBT automatically infers dependencies from `ref()` functions:

```sql
-- stg_customer_transactions references source
FROM {{ source('cleaned_data', 'customer_transactions') }}

-- dim_customers references staging
FROM {{ ref('stg_customer_transactions') }}

-- fact_transactions references dimensions
JOIN {{ ref('dim_customers') }} c
JOIN {{ ref('dim_products') }} p

-- agg_monthly_sales references fact
FROM {{ ref('fact_transactions') }}
```

---

## Performance Optimization

### Materialization Choices

**Views** (staging, aggregates):
- Always up-to-date
- No storage overhead
- Good for small/medium datasets
- Computed on-demand

**Tables** (dimensions, facts):
- Pre-computed results
- Fast query performance
- Requires rebuild
- Best for large datasets

---

## Troubleshooting

### Common Issues

**Issue**: dbt_utils undefined

```bash
# Install dependencies
make dbt-deps
```

**Issue**: Test failures

```bash
# View detailed test results
make dbt-test

# Check specific model
docker-compose exec airflow-webserver bash -c \
  "cd /opt/dbt && dbt test --select agg_monthly_sales --profiles-dir ."
```

**Issue**: Compilation errors

```bash
# Check SQL syntax
make dbt-compile

# Debug connection
make dbt-debug
```

---

## Next Steps

After successful transformation and aggregation:

1. ✅ Data is transformed into dimensional model
2. ✅ Data quality is validated through tests
3. ✅ Analytics-ready tables are available
4. → See **Stage 3: Data Orchestration** for Airflow configuration

See [Stage 3 Documentation](03-data-orchestration.md) for details on pipeline orchestration.

---

## Summary

**Stage 2** accomplishes:

✅ Transforms cleaned data into star schema <br>
✅ Creates reusable dimensions (customers, products) <br>
✅ Builds transaction fact table with proper relationships <br>
✅ Generates pre-aggregated metrics for reporting <br>
✅ Validates data quality with 147 automated tests <br>
✅ Provides analytics-ready data model for BI tools <br>
