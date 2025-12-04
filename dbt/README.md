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

## Models

### Staging Layer

#### `stg_customer_transactions`
- **Type**: View
- **Source**: `cleaned_data.customer_transactions`
- **Purpose**: Standardized view of cleaned transaction data with enhanced fields
- **Key Features**:
  - Date parsing and extraction (year, quarter, month)
  - Calculated fields (subtotal, total_amount)
  - Business validation flags
  - Null handling

### Dimension Layer

#### `dim_customers`
- **Type**: Table
- **Grain**: One row per customer
- **Purpose**: Customer dimension with aggregated metrics and segmentation
- **Key Metrics**:
  - Total transactions
  - Lifetime value
  - Average transaction value
  - Purchase frequency
  - Product diversity
- **Segmentations**:
  - Value Segment (VIP, High Value, Medium Value, Low Value)
  - Frequency Segment (Very Frequent, Frequent, Occasional, Rare)
  - Recency Segment (Active, Recent, Lapsed, Inactive)
  - Customer Tier (Platinum, Gold, Silver, Bronze)
  - AOV Tier (High AOV, Medium AOV, Low AOV)

#### `dim_products`
- **Type**: Table
- **Grain**: One row per product
- **Purpose**: Product dimension with aggregated sales metrics
- **Key Metrics**:
  - Total revenue
  - Total units sold
  - Average/min/max prices
  - Unique customers
  - Transaction frequency
- **Segmentations**:
  - Revenue Segment (Star Product, High/Medium/Low Performer)
  - Volume Segment (High/Medium/Low Volume)
  - Price Tier (Premium, Mid-Range, Value)

### Fact Layer

#### `fact_transactions`
- **Type**: Table
- **Grain**: One row per transaction
- **Purpose**: Transactional fact table with foreign keys to dimensions
- **Measures**:
  - Additive: units_sold, gross_amount, tax_amount, net_amount
  - Non-additive: unit_price
- **Foreign Keys**:
  - customer_fk → dim_customers.customer_key
  - product_fk → dim_products.product_key
- **Denormalized Attributes** (for performance):
  - customer_tier
  - customer_value_segment
  - product_name
  - product_price_tier
  - product_revenue_segment

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

### Recommended Additional Tests

```yaml
# In schema.yml
models:
  - name: dim_customers
    columns:
      - name: customer_key
        tests:
          - unique
          - not_null

  - name: dim_products
    columns:
      - name: product_key
        tests:
          - unique
          - not_null

  - name: fact_transactions
    columns:
      - name: transaction_key
        tests:
          - unique
          - not_null
      - name: customer_fk
        tests:
          - relationships:
              to: ref('dim_customers')
              field: customer_key
      - name: product_fk
        tests:
          - relationships:
              to: ref('dim_products')
              field: product_key
```

---

## Business Metrics Available

### Customer Analytics
- Customer Lifetime Value (CLV)
- Average Order Value (AOV)
- Purchase Frequency
- Customer Retention
- Customer Segmentation (RFM-like)
- Customer Acquisition Date

### Product Analytics
- Product Performance (Revenue, Units)
- Price Elasticity
- Product Mix Analysis
- Product Affinity
- Inventory Planning Metrics

### Transaction Analytics
- Daily/Monthly/Quarterly Sales
- Revenue by Segment
- Tax Analysis
- Average Transaction Size
- Seasonal Patterns

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
FROM analytics.dim_customers
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
FROM analytics.fact_transactions
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

## Integration with Airflow

This dbt project is orchestrated by Airflow using the `dbt_cosmos_dag`:

```python
# In airflow/dags/dbt_cosmos_dag.py
dbt_tg = DbtTaskGroup(
    group_id='dbt_transformations',
    project_config=ProjectConfig(
        dbt_project_path='/opt/dbt',
    ),
    profile_config=ProfileConfig(
        profile_name='ebury',
        target_name='dev',
    ),
    # ... additional configuration
)
```

---

## Troubleshooting

### Common Issues

**Issue**: `relation does not exist`
**Solution**: Ensure cleaned_data.customer_transactions table exists and is populated

**Issue**: `schema does not exist`
**Solution**: Create analytics schema: `CREATE SCHEMA IF NOT EXISTS analytics;`

**Issue**: Null values in foreign keys
**Solution**: Check `is_valid_transaction` filter in fact table

---

## Next Steps

1. **Add Date Dimension**: Create `dim_date` for time-based analysis
2. **Add Aggregates**: Create monthly/quarterly summary tables
3. **Add Snapshots**: Implement SCD Type 2 for historical tracking
4. **Add Macros**: Create reusable macros for common calculations
5. **Add Tests**: Implement comprehensive data quality tests
6. **Add Documentation**: Add descriptions for all columns and models
