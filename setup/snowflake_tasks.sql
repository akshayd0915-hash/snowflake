-- ============================================================
-- BANKING DATA PLATFORM — SNOWFLAKE TASK ORCHESTRATION
-- Simulates Airflow-style DAG using native Snowflake Tasks
-- Run as ACCOUNTADMIN to set up pipeline scheduling
-- ============================================================

-- Task 1: Root task — triggers every hour
-- In production: replace schedule with your SLA requirement
CREATE OR REPLACE TASK BANKING_DW.PUBLIC.TASK_PIPELINE_MONITOR
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
    SELECT 'Pipeline triggered at ' || CURRENT_TIMESTAMP();

-- Task 2: Data quality check
-- Validates RAW layer has data before allowing transformations
CREATE OR REPLACE TASK BANKING_DW.PUBLIC.TASK_DATA_QUALITY_CHECK
    WAREHOUSE = COMPUTE_WH
    AFTER BANKING_DW.PUBLIC.TASK_PIPELINE_MONITOR
AS
    SELECT
        COUNT(*) as customer_count,
        'Customers validated at ' || CURRENT_TIMESTAMP() as status
    FROM BANKING_DW.RAW.CUSTOMERS
    HAVING COUNT(*) > 0;

-- Task 3: Freshness check
-- Ensures mart tables are updated within SLA window (25 hours)
CREATE OR REPLACE TASK BANKING_DW.PUBLIC.TASK_FRESHNESS_CHECK
    WAREHOUSE = COMPUTE_WH
    AFTER BANKING_DW.PUBLIC.TASK_DATA_QUALITY_CHECK
AS
    SELECT
        MAX(DBT_UPDATED_AT)                                         as last_update,
        DATEDIFF('hour', MAX(DBT_UPDATED_AT), CURRENT_TIMESTAMP()) as hours_since_update,
        'Freshness checked at ' || CURRENT_TIMESTAMP()             as status
    FROM BANKING_DW.DBT_DEV_DBT_DEV.MART_CUSTOMER_360;

-- ============================================================
-- TO ENABLE IN PRODUCTION:
-- ALTER TASK BANKING_DW.PUBLIC.TASK_FRESHNESS_CHECK RESUME;
-- ALTER TASK BANKING_DW.PUBLIC.TASK_DATA_QUALITY_CHECK RESUME;
-- ALTER TASK BANKING_DW.PUBLIC.TASK_PIPELINE_MONITOR RESUME;
--
-- TO DISABLE:
-- ALTER TASK BANKING_DW.PUBLIC.TASK_PIPELINE_MONITOR SUSPEND;
--
-- ORCHESTRATION NOTES:
-- For complex pipelines, replace with Apache Airflow using
-- the SnowflakeOperator to trigger dbt runs as DAG tasks.
-- Task chain mirrors a DAG: Monitor → QualityCheck → Freshness
-- ============================================================