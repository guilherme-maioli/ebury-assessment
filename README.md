# Customer Transactions Data Pipeline

A production-ready end-to-end data pipeline that orchestrates CSV data ingestion, PostgreSQL loading, and DBT transformations using Apache Airflow.

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Running the Project Locally](#running-the-project-locally)
- [Pipeline Architecture](#pipeline-architecture)
- [Documentation](#documentation)
- [Key Features](#key-features)
- [Technologies Used](#technologies-used)

---

## Overview

This project implements a complete data pipeline that:

✅ **Ingests and cleans** raw CSV transaction data  
✅ **Loads data** efficiently into PostgreSQL using COPY command  
✅ **Transforms data** using DBT to create a dimensional model  
✅ **Orchestrates** the entire workflow with Apache Airflow  
✅ **Tests data quality** with comprehensive DBT tests  
✅ **Handles errors** with proper retry mechanisms and notifications

### Business Problem

Raw transaction data contains quality issues:
- Mixed date formats (dd-mm-yyyy and yyyy-mm-dd)
- Text values in numeric fields ("Two Hundred", "Fifteen")
- Prefixed identifiers ('T' and 'P' prefixes)
- Inconsistent data types

### Solution

A three-stage pipeline that:
1. **Cleans and validates** data before database insertion
2. **Transforms** data into a star schema with dimensions and facts
3. **Aggregates** metrics for analysis and reporting

---

## Project Structure

```
ebury/
├── airflow/
│   └── dags/
│       ├── pipeline_customer_transaction.py    # Main integrated pipeline
│       ├── csv_to_postgres_dag.py             # Raw data loading (legacy)
│       └── steps/                              # Modular pipeline components
│           ├── clean_transactions_pipeline.py  # Data cleaning logic
│           ├── load_to_postgres_pipeline.py    # PostgreSQL loading logic
│           ├── dbt_transformations_pipeline.py # DBT orchestration
│           ├── data_cleaners.py                # Cleaning utility functions
│           └── __init__.py
│
├── dbt/                                        # DBT project
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_customer_transactions.sql  # Staging layer
│   │   │   ├── schema.yml                     # Staging tests
│   │   │   └── source.yml                     # Source definitions
│   │   ├── dimensions/
│   │   │   ├── dim_customers.sql              # Customer dimension
│   │   │   ├── dim_products.sql               # Product dimension
│   │   │   └── schema.yml                     # Dimension tests
│   │   ├── facts/
│   │   │   ├── fact_transactions.sql          # Transaction fact table
│   │   │   └── schema.yml                     # Fact tests
│   │   └── aggregates/
│   │       ├── agg_monthly_sales.sql          # Monthly aggregates
│   │       └── schema.yml                     # Aggregate tests
│   ├── dbt_project.yml                        # DBT configuration
│   ├── profiles.yml                           # Database connections
│   └── packages.yml                           # DBT dependencies
│
├── data/
│   ├── customer_transactions.csv              # Raw input data
│   └── customer_transactions_cleaned.csv      # Cleaned output
│
├── docs/                                       # Project documentation
│   ├── 01-data-ingestion-preparation.md       # Stage 1 documentation
│   ├── 02-data-transformation-aggregation.md  # Stage 2 documentation
│   └── 03-data-orchestration.md               # Stage 3 documentation
│
├── docker-compose.yml                          # Services configuration
├── Makefile                                    # Command shortcuts
└── README.md                                   # This file
```

---

## Running the Project Locally

### Prerequisites

- Docker and Docker Compose
- Make (optional, for command shortcuts)

### Quick Start

1. **Clone the repository**:
   ```bash
   git clone https://github.com/guilherme-maioli/ebury-assessment.git
   cd ebury
   ```

2. **Start all services**:
   ```bash
   make up
   # OR
   docker-compose up -d
   ```

   This starts:
   - Apache Airflow (webserver and scheduler)
   - PostgreSQL database

3. **Access Airflow UI**:
   - URL: http://localhost:8081
   - Username: `airflow`
   - Password: `airflow`

4. **Install DBT dependencies** (optional, because Dag do automatically):
   ```bash
   make dbt-deps
   ```

5. **Monitor execution**:
   ```bash
   make logs-airflow
   # OR view in Airflow UI Graph view
   ```

### Useful Commands

| Command | Description |
|---------|-------------|
| `make help` | Show all available commands |
| `make up` | Start all services |
| `make down` | Stop all services |
| `make restart` | Restart services |
| `make logs` | Show logs from all services |
| `make status` | Show container status |
| `make shell-airflow` | Open shell in Airflow container |
| `make shell-postgres` | Open PostgreSQL shell |
| `make airflow-trigger-pipeline` | Trigger main pipeline |
| `make airflow-dags-list` | List all DAGs |
| `make dbt-deps` | Install DBT dependencies |
| `make dbt-run` | Run DBT models manually |
| `make dbt-test` | Run DBT tests |

---

## Pipeline Architecture

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────┐
│           pipeline_customer_transaction DAG                  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ STAGE 1: Data Ingestion & Preparation                       │
├─────────────────────────────────────────────────────────────┤
│ TaskGroup: Clean-Transactions                               │
│   └─ clean_and_transform_csv                                │
│      • Read: data/customer_transactions.csv                 │
│      • Clean and validate data                              │
│      • Output: data/customer_transactions_cleaned.csv       │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ STAGE 1: Data Ingestion & Preparation (continued)           │
├─────────────────────────────────────────────────────────────┤
│ TaskGroup: Load-To-Postgres                                 │
│   ├─ create_cleaned_table                                   │
│   ├─ truncate_cleaned_table                                 │
│   ├─ load_cleaned_csv_to_postgres (COPY command)            │
│   └─ validate_postgres_load                                 │
│      → Target: cleaned_data.customer_transactions           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ STAGE 2: Data Transformation & Aggregation                  │
├─────────────────────────────────────────────────────────────┤
│ TaskGroup: DBT-Transformations (Cosmos)                     │
│   └─ dbt_models (Auto-generated tasks)                      │
│      ├─ stg_customer_transactions (Staging)                 │
│      ├─ dim_customers (Dimension)                           │
│      ├─ dim_products (Dimension)                            │
│      ├─ fact_transactions (Fact)                            │
│      └─ agg_monthly_sales (Aggregate)                       │
│         Each with build + test tasks                        │
└─────────────────────────────────────────────────────────────┘
```

### Data Model

```
┌──────────────────┐         ┌──────────────────┐
│  dim_customers   │         │   dim_products   │
├──────────────────┤         ├──────────────────┤
│ customer_key (PK)│         │ product_key (PK) │
│ customer_id      │         │ product_id       │
│ customer_tier    │         │ product_name     │
│ value_segment    │         │ price_tier       │
│ first_seen_at    │         │ revenue_segment  │
│ last_seen_at     │         │ first_sold_at    │
└──────────────────┘         └──────────────────┘
         │                            │
         │                            │
         └─────────┬──────────────────┘
                   │
                   ↓
         ┌──────────────────────┐
         │  fact_transactions   │
         ├──────────────────────┤
         │ transaction_key (PK) │
         │ customer_fk (FK)     │
         │ product_fk (FK)      │
         │ transaction_date     │
         │ units_sold           │
         │ gross_amount         │
         │ tax_amount           │
         │ net_amount           │
         │ ...                  │
         └──────────────────────┘
                   │
                   ↓
         ┌──────────────────────┐
         │  agg_monthly_sales   │
         ├──────────────────────┤
         │ customer_fk          │
         │ transaction_month    │
         │ transaction_count    │
         │ monthly_net_revenue  │
         │ revenue_growth_pct   │
         │ ...                  │
         └──────────────────────┘
```

---

## Documentation

Detailed documentation is available in the `/docs` directory:

### [1. Data Ingestion and Preparation](docs/01-data-ingestion-preparation.md)
- CSV data cleaning process
- Data validation and type conversion
- PostgreSQL COPY command usage
- NULL handling strategies
- Error handling and validation

### [2. Data Transformation and Aggregation](docs/02-data-transformation-aggregation.md)
- DBT project structure
- Staging layer design
- Dimensional modeling 
- Fact table implementation
- Monthly aggregations with growth metrics
- Data quality tests

### [3. Data Orchestration](docs/03-data-orchestration.md)
- Airflow DAG structure
- Modular pipeline architecture with `steps/`
- TaskGroup organization
- Scheduling and execution
- Error handling and retries
- Failure notifications
- Database connections via docker-compose

---

## Key Features

### 1. Modular Architecture

Pipeline logic is separated into reusable modules in `airflow/dags/steps/`:

```python
# Each module follows this pattern
def pipeline_module(parent_group):
    # Define tasks
    # Set dependencies
    # Return final task
```

**Benefits**:
- Testable components
- Reusable across DAGs
- Clear separation of concerns
- Easy to maintain

### 2. Efficient Data Loading

Uses PostgreSQL COPY command for bulk loading:
- **faster** than INSERT statements
- Streaming: no memory overhead
- Atomic: transaction safety
- Handles large datasets efficiently

### 3. Comprehensive Data Quality

**At ingestion**:
- Type validation
- Format standardization
- Duplicate detection
- NULL handling

**At transformation**:
- DBT tests across all models
- Unique constraints
- Foreign key validation
- Business logic checks
- Range validations

### 4. Dimensional Modeling

Star schema design:
- **Staging**: Cleaned source data
- **Dimensions**: Customer and Product 
- **Facts**: Transaction-level data
- **Aggregates**: Pre-calculated metrics

### 5. Automated Dependencies

- **Cosmos** auto-generates Airflow tasks from DBT models
- Dependencies inferred from `ref()` functions
- No manual task creation needed
- Full lineage tracking

---

## Technologies Used

| Technology | Purpose |
|------------|---------|
| **Apache Airflow** | Workflow orchestration |
| **DBT** | Data transformation | 
| **PostgreSQL** | Data warehouse | 
| **Python** | Data processing | 
| **Pandas** | Data manipulation | 
| **Astronomer Cosmos** | DBT-Airflow integration | 
| **Docker** | Containerization | 
| **Docker Compose** | Multi-container orchestration | 

---

## Configuration

### Environment Variables

Create a `.env` file:
```bash
# Airflow Configuration
AIRFLOW_UID=1000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# PostgreSQL Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# dbt Configuration
DBT_PROFILES_DIR=/opt/dbt
DBT_PROJECT_DIR=/opt/dbt

# Logging
AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
```

### Database Connection

Configured in `docker-compose.yml`:
```yaml
AIRFLOW_CONN_POSTGRES_DEFAULT: 'postgresql://airflow:airflow@postgres:5432/ebury'
```
Observation: I created ebury database manually.

### DBT Profiles

Located at `dbt/profiles.yml`:
```yaml
ebury:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres
      user: airflow
      password: airflow
      dbname: ebury
      schema: analytics_dbt_staging
      port: 5432
```

---
