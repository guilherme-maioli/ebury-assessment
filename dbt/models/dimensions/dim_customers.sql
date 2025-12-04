{{
    config(
        materialized='table',
        tags=['dimensions', 'customers']
    )
}}

/*
    Dimension Model: Customers
    ==========================

    Purpose:
    - Create a customer dimension table with SCD Type 1
    - Aggregate customer-level metrics
    - Support customer segmentation and analysis

    Grain: One row per customer
*/

WITH customer_transactions AS (
    SELECT
        customer_id,
        transaction_date,
        transaction_year,
        transaction_month,
        quantity,
        unit_price,
        total_amount,
        is_valid_transaction
    FROM {{ ref('stg_customer_transactions') }}
    WHERE is_valid_transaction = TRUE
),

customer_metrics AS (
    SELECT
        customer_id,

        -- Transaction metrics
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT transaction_date) AS unique_transaction_days,
        COUNT(DISTINCT transaction_year) AS years_active,
        COUNT(DISTINCT transaction_month) AS months_active,

        -- Date ranges
        MIN(transaction_date) AS first_transaction_date,
        MAX(transaction_date) AS last_transaction_date,

        -- Sales metrics
        SUM(quantity) AS total_units_purchased,
        SUM(total_amount) AS total_lifetime_value,
        AVG(total_amount) AS avg_transaction_value,
        MIN(total_amount) AS min_transaction_value,
        MAX(total_amount) AS max_transaction_value,

        -- Product diversity
        COUNT(DISTINCT product_id) AS unique_products_purchased

    FROM {{ ref('stg_customer_transactions') }}
    WHERE is_valid_transaction = TRUE
    GROUP BY customer_id
),

customer_segments AS (
    SELECT
        *,

        -- Customer value segmentation (RFM-like)
        CASE
            WHEN total_lifetime_value >= 5000 THEN 'VIP'
            WHEN total_lifetime_value >= 2000 THEN 'High Value'
            WHEN total_lifetime_value >= 500 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS value_segment,

        -- Purchase frequency
        CASE
            WHEN total_transactions >= 20 THEN 'Very Frequent'
            WHEN total_transactions >= 10 THEN 'Frequent'
            WHEN total_transactions >= 5 THEN 'Occasional'
            ELSE 'Rare'
        END AS frequency_segment,

        -- Recency
        CURRENT_DATE - last_transaction_date AS days_since_last_purchase,

        CASE
            WHEN CURRENT_DATE - last_transaction_date <= 7 THEN 'Active'
            WHEN CURRENT_DATE - last_transaction_date <= 30 THEN 'Recent'
            WHEN CURRENT_DATE - last_transaction_date <= 90 THEN 'Lapsed'
            ELSE 'Inactive'
        END AS recency_segment,

        -- Customer lifetime (days since first purchase)
        CURRENT_DATE - first_transaction_date AS customer_age_days,

        -- Average order value tier
        CASE
            WHEN avg_transaction_value >= 500 THEN 'High AOV'
            WHEN avg_transaction_value >= 200 THEN 'Medium AOV'
            ELSE 'Low AOV'
        END AS aov_tier,

        -- Combined customer tier (based on value + frequency)
        CASE
            WHEN total_lifetime_value >= 5000 AND total_transactions >= 15 THEN 'Platinum'
            WHEN total_lifetime_value >= 2000 AND total_transactions >= 10 THEN 'Gold'
            WHEN total_lifetime_value >= 500 AND total_transactions >= 5 THEN 'Silver'
            ELSE 'Bronze'
        END AS customer_tier

    FROM customer_metrics
)

SELECT
    -- Primary key
    customer_id AS customer_key,

    -- Customer identifier
    customer_id,

    -- Transaction metrics
    total_transactions,
    unique_transaction_days,
    years_active,
    months_active,
    unique_products_purchased,

    -- Date information
    first_transaction_date,
    last_transaction_date,
    days_since_last_purchase,
    customer_age_days,

    -- Value metrics
    total_lifetime_value,
    avg_transaction_value,
    min_transaction_value,
    max_transaction_value,
    total_units_purchased,

    -- Segmentation
    value_segment,
    frequency_segment,
    recency_segment,
    aov_tier,
    customer_tier,

    -- Audit fields
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at

FROM customer_segments
