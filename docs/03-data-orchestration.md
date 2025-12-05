# Stage 3: Data Orchestration

This document describes how the entire pipeline is orchestrated using Apache Airflow, including DAG structure, scheduling, error handling, and configuration.

## Table of Contents

- [Overview](#overview)
- [Airflow Architecture](#airflow-architecture)
- [DAG Structure](#dag-structure)
- [Modular Pipeline Design](#modular-pipeline-design)
- [TaskGroup Organization](#taskgroup-organization)
- [Scheduling and Execution](#scheduling-and-execution)
- [Error Handling and Retries](#error-handling-and-retries)
- [Database Connections](#database-connections)
- [Monitoring and Observability](#monitoring-and-observability)

---

## Overview

**Orchestration Tool**: Apache Airflow 2.7.3 <br>
**Execution Mode**: Local Executor <br>
**Scheduler**: Daily (`@daily`) <br>
**Main DAG**: `pipeline_customer_transaction` <br>

### Architecture Components

```
┌────────────────────────────────────┐
│     Docker Compose Environment     │
├────────────────────────────────────┤
│  ┌──────────────┐  ┌─────────────┐ │
│  │   Airflow    │  │ PostgreSQL  │ │
│  │  Webserver   │  │  Database   │ │
│  └──────────────┘  └─────────────┘ │
│  ┌──────────────┐                  │
│  │   Airflow    │                  │
│  │  Scheduler   │                  │
│  └──────────────┘                  │
└────────────────────────────────────┘
```

---

## Airflow Architecture

### Services

Defined in `docker-compose.yml`:

**1. PostgreSQL Service**
```yaml
postgres:
  image: postgres:13
  environment:
    POSTGRES_USER: airflow
    POSTGRES_PASSWORD: airflow
    POSTGRES_DB: airflow
  ports:
    - "5432:5432"
```

**Serves two purposes**:
- Airflow metadata database
- Target database for pipeline (`ebury` database created manually)

**2. Airflow Webserver**
```yaml
airflow-webserver:
  image: apache/airflow:2.7.3-python3.9
  command: webserver
  ports:
    - "8081:8080"
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW_CONN_POSTGRES_DEFAULT: 'postgresql://airflow:airflow@postgres:5432/ebury'
```

**Functions**:
- Provides web UI (http://localhost:8081)
- DAG management and visualization
- Task monitoring and logs

**3. Airflow Scheduler**
```yaml
airflow-scheduler:
  command: scheduler
```

**Functions**:
- Triggers DAG runs based on schedule
- Manages task execution
- Handles retries and dependencies

### Volume Mounts

```yaml
volumes:
  - ./airflow/dags:/opt/airflow/dags      # DAG files
  - ./airflow/logs:/opt/airflow/logs      # Execution logs
  - ./dbt:/opt/dbt                        # DBT project
  - ./data:/opt/data                      # CSV files
```

---

## DAG Structure

### Main DAG: pipeline_customer_transaction

**File**: `airflow/dags/pipeline_customer_transaction.py`

```python
@dag(**dag_definition)
def clean_customer_transactions():
    """
    Main DAG function that orchestrates the cleaning, loading, and transformation pipeline.
    """

    # TaskGroup 1: Clean-Transactions
    with TaskGroup(group_id="Clean-Transactions") as clean_tg:
        clean_transactions_pipeline(parent_group)

    # TaskGroup 2: Load-To-Postgres
    with TaskGroup(group_id="Load-To-Postgres") as load_tg:
        load_to_postgres_pipeline(parent_group)

    # TaskGroup 3: DBT-Transformations
    with TaskGroup(group_id="DBT-Transformations") as dbt_tg:
        dbt_transformations_pipeline(parent_group)

    # Define dependencies
    clean_tg >> load_tg >> dbt_tg

dag_instance = clean_customer_transactions()
```

### DAG Configuration

```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag_definition = {
    'dag_id': 'pipeline_customer_transaction',
    'default_args': default_args,
    'description': 'Complete data pipeline: CSV cleaning, PostgreSQL loading, and DBT transformations',
    'schedule_interval': '@daily',
    'catchup': False,
    'tags': ['data-pipeline', 'csv', 'postgres', 'dbt', 'end-to-end'],
}
```

**Key Settings**:
- `depends_on_past`: False - Each run is independent
- `catchup`: False - Don't backfill missed runs
- `retries`: 1 - Retry failed tasks once
- `retry_delay`: 2 minutes between retries
- `schedule_interval`: Daily at midnight

---

## Modular Pipeline Design

### The `steps/` Directory

Pipeline logic is organized in reusable modules:

```
airflow/dags/steps/
├── __init__.py
├── data_cleaners.py                 # Utility functions
├── clean_transactions_pipeline.py   # Cleaning logic
├── load_to_postgres_pipeline.py     # Loading logic
└── dbt_transformations_pipeline.py  # DBT orchestration
```

### Module Pattern

Each module follows this structure:

```python
def pipeline_module(parent_group):
    """
    Reusable pipeline component.

    Args:
        parent_group: Task group ID for logging context

    Returns:
        Final task in the pipeline
    """

    @task(task_id="task_name")
    def task_function(upstream_output=None):
        # Task implementation
        logger.info(f"[{parent_group}] Executing task")
        return result

    # Create tasks with implicit dependencies
    task1 = task_function()
    task2 = another_task(task1)  # Dependency via argument

    return task2
```

**Benefits**:
1. **Reusability**: Import in multiple DAGs
2. **Testability**: Unit test individual modules
3. **Maintainability**: Update logic without changing DAG
4. **Separation of Concerns**: DAG structure vs business logic
5. **Clarity**: Clear boundaries and responsibilities

### Dependency Management

Uses **XCom argument passing** for task dependencies:

```python
# Tasks accept upstream output to create implicit dependencies
@task(task_id="truncate_table")
def truncate_table(upstream_output):
    # upstream_output from previous task
    pass

@task(task_id="load_data")
def load_data(upstream_output):
    # upstream_output from truncate_table
    pass

# Dependencies created by passing outputs
create = create_table()
truncate = truncate_table(create)      # Depends on create
load = load_data(truncate)             # Depends on truncate
```

**Why this approach?**
- Works with `@dag` decorator
- Explicit data flow
- No manual dependency setting needed
- Type-safe with proper IDE support

---

## TaskGroup Organization

### TaskGroup 1: Clean-Transactions

**Purpose**: Data cleaning and validation
**Module**: `clean_transactions_pipeline.py`
**Tasks**: 1 task

```
Clean-Transactions
└── clean_and_transform_csv
    • Reads raw CSV
    • Applies cleaning functions
    • Validates data types
    • Outputs cleaned CSV
```

### TaskGroup 2: Load-To-Postgres

**Purpose**: Database loading and validation
**Module**: `load_to_postgres_pipeline.py`
**Tasks**: 4 tasks

```
Load-To-Postgres
├── create_cleaned_table
│   └── Creates schema and table
├── truncate_cleaned_table
│   └── Ensures idempotency
├── load_cleaned_csv_to_postgres
│   └── COPY command for bulk loading
└── validate_postgres_load
    └── Data quality checks
```

### TaskGroup 3: DBT-Transformations

**Purpose**: Dimensional modeling
**Module**: `dbt_transformations_pipeline.py`
**Tasks**: Auto-generated by Cosmos

```
DBT-Transformations
└── dbt_models
    ├── stg_customer_transactions
    │   ├── run
    │   └── test
    ├── dim_customers
    │   ├── run
    │   └── test
    ├── dim_products
    │   ├── run
    │   └── test
    ├── fact_transactions
    │   ├── run
    │   └── test
    └── agg_monthly_sales
        ├── run
        └── test
```

**Cosmos Integration**:

```python
dbt_tg = DbtTaskGroup(
    group_id='dbt_models',
    project_config=ProjectConfig(
        dbt_project_path='/opt/dbt',
    ),
    profile_config=ProfileConfig(
        profile_name='ebury',
        target_name='dev',
        profiles_yml_filepath='/opt/dbt/profiles.yml',
    ),
    execution_config=ExecutionConfig(
        dbt_executable_path='/home/airflow/.local/bin/dbt',
    ),
    render_config=RenderConfig(
        dbt_deps=True,  # Auto-install dependencies
    ),
    operator_args={
        'install_deps': True,
    },
)
```

**Cosmos Benefits**:
- Auto-generates tasks from DBT project
- Infers dependencies from `ref()` functions
- Includes both run and test tasks
- Full lineage tracking
- No manual task creation needed

---

## Scheduling and Execution

### Schedule Interval

```python
'schedule_interval': '@daily'  # Runs every day at midnight
```

**Other common intervals**:
```python
'@hourly'    # Every hour
'@daily'     # Every day at midnight
'@weekly'    # Every Sunday at midnight
'@monthly'   # First day of month at midnight
'0 8 * * *'  # Every day at 8 AM (cron format)
None         # Manual trigger only
```

### Execution Process

1. **Scheduler triggers DAG** at scheduled time
2. **Clean-Transactions TaskGroup** executes
   - Cleans CSV data
   - Outputs cleaned file
3. **Load-To-Postgres TaskGroup** executes
   - Creates/truncates table
   - Loads data with COPY
   - Validates load
4. **DBT-Transformations TaskGroup** executes
   - Installs dbt_utils (if needed)
   - Runs staging model
   - Builds dimensions in parallel
   - Creates fact table
   - Generates aggregates
   - Runs all tests

### Parallelism

Tasks within same level can run in parallel:

```
stg_customer_transactions
    ├──→ dim_customers (parallel)
    └──→ dim_products  (parallel)
            ↓
    fact_transactions (waits for both)
            ↓
    agg_monthly_sales
```

**Configuration**:
```python
# In docker-compose.yml
_PIP_ADDITIONAL_REQUIREMENTS: 'dbt-core==1.7.4 dbt-postgres==1.7.4 pandas==2.1.4 astronomer-cosmos'

# In Airflow config
AIRFLOW__CORE__EXECUTOR: LocalExecutor  # Supports parallelism
```

---

## Error Handling and Retries

### Retry Configuration

```python
default_args = {
    'retries': 1,                           # Retry once
    'retry_delay': timedelta(minutes=2),    # Wait 2 minutes
}
```

**Retry Behavior**:
1. Task fails
2. Wait 2 minutes
3. Retry once
4. If fails again, mark as failed
5. Downstream tasks don't execute


### Failure Callbacks

Can add failure notifications:

```python
def notification_failed_task_slack(context):
    """Send Slack notification on task failure"""
    # Implementation here
    pass

default_args = {
    'on_failure_callback': notification_failed_task_slack
}
```

**Context includes**:
- `task_instance`: Failed task details
- `exception`: Error message
- `dag_run`: DAG run information
- `execution_date`: When task ran

---

## Database Connections

### Connection Configuration

Connections are configured directly in `docker-compose.yml`:

```yaml
environment:
  # Airflow metadata database
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow

  # Target database for pipeline
  AIRFLOW_CONN_POSTGRES_DEFAULT: 'postgresql://airflow:airflow@postgres:5432/ebury'
```

**Format**:
```
postgresql://[user]:[password]@[host]:[port]/[database]
```

### Using Connections in Code

**PostgreSQL Hook**:
```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

pg_hook = PostgresHook(postgres_conn_id='postgres_default')

# Execute query
pg_hook.run("CREATE TABLE ...")

# Get connection
conn = pg_hook.get_conn()
cursor = conn.cursor()
```

### DBT Connection

Configured in `dbt/profiles.yml`:

```yaml
ebury:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres          # Docker service name
      user: airflow
      password: airflow
      dbname: ebury
      schema: analytics_dbt_staging
      port: 5432
      threads: 4
```

**Note**: Uses Docker service name `postgres` for networking

---

## Monitoring and Observability

### Airflow UI

**Access**: http://localhost:8081
**Credentials**: airflow / airflow

**Key Views**:

1. **DAGs View**: List all DAGs
   - Enable/disable DAGs
   - Trigger manual runs
   - View schedule and last run

2. **Graph View**: Visual task dependencies
   - See task status (success/failure/running)
   - Click tasks to view logs
   - View task duration

3. **Gantt Chart**: Timeline view
   - See task execution times
   - Identify bottlenecks
   - Analyze parallelism

4. **Task Instance**: Individual task details
   - View logs
   - Check XCom values
   - Retry/mark success/clear

### Logging

**Location**: `airflow/logs/`

**Structure**:
```
logs/
└── dag_id=pipeline_customer_transaction/
    └── run_id=manual__2025-12-04T10:00:00/
        └── task_id=Clean-Transactions.clean_and_transform_csv/
            └── attempt=1.log
```

**View logs**:
```bash
# Via Makefile
make logs-airflow

# Via Airflow UI
# Click task → View Log

# Via shell
make shell-airflow
cat /opt/airflow/logs/pipeline_customer_transaction/.../attempt=1.log
```

### Metrics and Monitoring

**Task Duration**:
- View in Gantt chart
- Identify slow tasks
- Optimize bottlenecks

**Success Rate**:
- DAGs view shows success/failure counts
- Click DAG for historical runs
- Tree view shows run history

---

## Operational Commands

### Via Makefile

```bash
# Start services
make up

# Monitor execution
make logs-airflow

# Stop services
make down

# restart
make restart
```

### Via Airflow CLI

```bash
# Enter Airflow container
make shell-airflow

# Trigger DAG
airflow dags trigger pipeline_customer_transaction

# List DAGs
airflow dags list

# Test task
airflow tasks test pipeline_customer_transaction \
  Clean-Transactions.clean_and_transform_csv 2025-12-04

# View task state
airflow tasks state pipeline_customer_transaction \
  Clean-Transactions.clean_and_transform_csv 2025-12-04
```

---

## Troubleshooting

### DAG Not Appearing

```bash
# Check scheduler logs
make logs-scheduler

# Restart scheduler
docker-compose restart airflow-scheduler
```

### Task Failures

```bash
# View task logs in UI
# Click task → View Log

# Or via command line
make logs-airflow | grep "ERROR"


# Manually retry task
# In UI: Click task → Clear → Confirm
```

### Connection Issues

```bash
# Test PostgreSQL connection
make shell-postgres

# Check connection string
docker-compose exec airflow-webserver env | grep AIRFLOW_CONN

# Verify DBT connection
make dbt-debug
```

---

## Best Practices

### 1. Idempotency

All tasks should be idempotent (safe to rerun):

```python
# Always use TRUNCATE before loading
TRUNCATE TABLE cleaned_data.customer_transactions;

# Or use CREATE TABLE IF NOT EXISTS
CREATE TABLE IF NOT EXISTS ...
```

### 2. Explicit Dependencies

Use XCom argument passing for clear dependencies:

```python
# Clear dependency chain
task1 = create_table()
task2 = load_data(task1)
task3 = validate_data(task2)
```

### 3. Comprehensive Logging

Add context to log messages:

```python
logger.info(f"Processing {len(df)} rows")
logger.info(f"Loaded {rows_loaded} rows in {duration}s")
```

### 4. Error Handling

Always use try/except/finally:

```python
try:
    conn.commit()
except Exception as e:
    conn.rollback()
    raise
finally:
    conn.close()
```

### 5. Resource Management

Clean up resources:

```python
finally:
    cursor.close()
    conn.close()
```

---

## Summary

**Stage 3** provides:

✅ Automated workflow orchestration with Airflow <br>
✅ Modular pipeline design with `steps/` modules <br>
✅ Clear task organization with TaskGroups <br>
✅ Automatic DBT task generation with Cosmos <br>
✅ Robust error handling and retries <br>
✅ Easy database connection management via docker-compose <br>
✅ Comprehensive monitoring and observability <br>
✅ Simple operations via Makefile commands <br>

The complete pipeline runs daily, processing data from CSV through cleaning, loading, transformation, and aggregation, with full visibility and control through the Airflow UI.
