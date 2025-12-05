# DBT Customer Transactions Data Warehouse

## Overview

This dbt project transforms cleaned customer transaction data into a dimensional data warehouse suitable for analytics and reporting.

---

## Data Model Architecture

### Star Schema Design

```
          ┌─────────────────┐
          │  dim_customers  │
          │  (Dimension)    │
          └────────┬────────┘
                   │
                   │
    ┌──────────────┼──────────────┐
    │              │              │
    │       ┌──────▼──────┐       │
    │       │fact_trans   │       │
    │       │actions      │       │
    │       │   (Fact)    │       │
    │       └──────┬──────┘       │
    │              │              │
    │              │              │
┌───▼──────────┐   │   ┌──────────▼───┐
│ dim_products │   │   │   dim_date   │
│ (Dimension)  │   │   │ (Dimension)  │
└──────────────┘   │   └──────────────┘
                   │
```

---


## Data Flow

```
cleaned_data.customer_transactions (PostgreSQL)
           ↓
    stg_customer_transactions (View)
           ↓
    ┌──────┴──────┐
    ↓             ↓
dim_customers  dim_products
    ↓             ↓
    └──────┬──────┘
           ↓
    fact_transactions
```

---

## Running DBT

### Prerequisites

Ensure your `profiles.yml` is configured with PostgreSQL connection:

```yaml
ebury:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres
      user: airflow
      password: airflow
      port: 5432
      dbname: airflow
      schema: analytics
      threads: 4
```

### Commands

```bash
# Install dependencies (if any)
dbt deps

# Run all models
dbt run

# Run specific models
dbt run --select stg_customer_transactions
dbt run --select dim_customers
dbt run --select dim_products
dbt run --select fact_transactions

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve

# Run with full refresh
dbt run --full-refresh
```

---

## Model Dependencies

```
stg_customer_transactions
  ├── dim_customers (depends on staging)
  ├── dim_products (depends on staging)
  └── fact_transactions (depends on dim_customers, dim_products)
```

---

## Data Quality Tests

### Source Tests

Tests defined in `source.yml`:
- `transaction_id`: not_null
- `transaction_date`: not_null
- `product_id`: not_null
- `price`: not_null
- `tax`: not_null

---

## Example Queries

### Top 10 Customers by Lifetime Value

```sql
SELECT
    customer_id,
    customer_tier,
    total_lifetime_value,
    total_transactions,
    avg_transaction_value
FROM analytics_dbt_analytics.dim_customers
ORDER BY total_lifetime_value DESC
LIMIT 10;
```

### Monthly Revenue Trend

```sql
SELECT
    transaction_year,
    transaction_month,
    SUM(net_amount) AS total_revenue,
    COUNT(*) AS transaction_count,
    AVG(net_amount) AS avg_transaction_value
FROM analytics_dbt_analytics.fact_transactions
GROUP BY transaction_year, transaction_month
ORDER BY transaction_year, transaction_month;
```

### Product Performance by Customer Tier

```sql
SELECT
    f.product_name,
    f.customer_tier,
    COUNT(*) AS transactions,
    SUM(f.net_amount) AS revenue,
    AVG(f.net_amount) AS avg_revenue
FROM analytics.fact_transactions f
GROUP BY f.product_name, f.customer_tier
ORDER BY revenue DESC;
```

---

## Incremental Loading (Future Enhancement)

The `fact_transactions` model includes commented code for incremental loading:

```sql
{% if is_incremental() %}
  WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
{% endif %}
```

To enable:
1. Change materialization to `incremental`
2. Uncomment the incremental logic
3. Add `unique_key='transaction_key'`

---

## Maintenance

### Full Refresh
Run when schema changes or data integrity issues occur:
```bash
dbt run --full-refresh
```

### Selective Refresh
Rebuild specific models:
```bash
dbt run --select dim_customers+ # Rebuild dim_customers and all downstream
dbt run --select +fact_transactions # Rebuild all upstream and fact_transactions
```

---

## Performance Optimization

### Current Optimizations
- Dimensions materialized as tables for fast joins
- Staging layer as view to save storage
- Denormalized attributes in fact table for common queries

### Future Enhancements
1. **Partitioning**: Partition fact table by transaction_date
2. **Indexes**: Add indexes on foreign keys and date columns
3. **Aggregates**: Create pre-aggregated summary tables
4. **Incremental Models**: Convert to incremental for large datasets

---

## Next Steps

1. **Add Date Dimension**: Create `dim_date` for time-based analysis
2. **Add Aggregates**: Create monthly/quarterly summary tables
3. **Add Snapshots**: Implement for historical tracking
4. **Add Macros**: Create reusable macros for common calculations
5. **Add Tests**: Implement comprehensive data quality tests
6. **Add Documentation**: Add descriptions for all columns and models
