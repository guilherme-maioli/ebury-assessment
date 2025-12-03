-- ============================================================================
-- Create schemas in the default airflow database
-- ============================================================================

-- Create the raw_data schema
CREATE SCHEMA IF NOT EXISTS raw_data;

-- Create additional schemas for your data pipeline
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS raw;

-- Grant permissions to the airflow user on all schemas
GRANT ALL PRIVILEGES ON SCHEMA raw_data TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA staging TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA raw TO airflow;

-- Grant usage and create permissions
GRANT USAGE, CREATE ON SCHEMA raw_data TO airflow;
GRANT USAGE, CREATE ON SCHEMA staging TO airflow;
GRANT USAGE, CREATE ON SCHEMA analytics TO airflow;
GRANT USAGE, CREATE ON SCHEMA raw TO airflow;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_data GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO airflow;

-- ============================================================================
-- Optionally create the ebury database (commented out by default)
-- Uncomment if you want a separate ebury database
-- ============================================================================
-- SELECT 'CREATE DATABASE ebury'
-- WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'ebury')\gexec

-- Log completion
DO $$
BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Database initialization completed successfully!';
    RAISE NOTICE 'Created schemas in airflow database:';
    RAISE NOTICE '  - raw_data';
    RAISE NOTICE '  - staging';
    RAISE NOTICE '  - analytics';
    RAISE NOTICE '  - raw';
    RAISE NOTICE '============================================================';
END $$;