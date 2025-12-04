{{
    config(
        materialized='view',
        tags=['aggregates', 'monthly-sales']
    )
}}

/*
    Aggregate Model: Monthly Sales
    ==============================
    
    Purpose:
    - Provide pre-aggregated monthly sales metrics
    - Support time-series analysis and reporting
    - Optimize query performance for monthly dashboards
    
    Grain: One row per customer per month
*/

WITH monthly_transactions AS (
    SELECT
        customer_fk,
        transaction_month,
        transaction_year,
        transaction_quarter,
        
        -- Transaction counts
        COUNT(*) AS transaction_count,
        COUNT(DISTINCT product_fk) AS unique_products_purchased,
        
        -- Quantity metrics
        SUM(units_sold) AS total_units,
        AVG(units_sold) AS avg_units_per_transaction,
        
        -- Revenue metrics
        SUM(gross_amount) AS monthly_gross_revenue,
        SUM(tax_amount) AS monthly_tax,
        SUM(net_amount) AS monthly_net_revenue,
        SUM(revenue_excluding_tax) AS monthly_revenue_excluding_tax,
        
        -- Price analytics
        AVG(unit_price) AS avg_unit_price,
        MIN(unit_price) AS min_unit_price,
        MAX(unit_price) AS max_unit_price,
        
        -- Customer attributes (taking most recent)
        MAX(customer_tier) AS customer_tier,
        MAX(customer_value_segment) AS customer_value_segment
        
    FROM {{ ref('fact_transactions') }}
    GROUP BY
        customer_fk,
        transaction_month,
        transaction_year,
        transaction_quarter
),

monthly_with_growth AS (
    SELECT
        *,
        
        -- Calculate month-over-month growth
        LAG(monthly_net_revenue) OVER (
            PARTITION BY customer_fk 
            ORDER BY transaction_month
        ) AS previous_month_revenue,
        
        LAG(transaction_count) OVER (
            PARTITION BY customer_fk 
            ORDER BY transaction_month
        ) AS previous_month_transactions,
        
        -- Calculate average revenue per transaction
        CASE
            WHEN transaction_count > 0
            THEN monthly_net_revenue / transaction_count
            ELSE 0
        END AS avg_revenue_per_transaction
        
    FROM monthly_transactions
)

SELECT
    -- Composite key
    customer_fk,
    transaction_month,
    transaction_year,
    transaction_quarter,
    
    -- Transaction metrics
    transaction_count,
    unique_products_purchased,
    total_units,
    avg_units_per_transaction,
    
    -- Revenue metrics
    monthly_gross_revenue,
    monthly_tax,
    monthly_net_revenue,
    monthly_revenue_excluding_tax,
    avg_revenue_per_transaction,
    
    -- Price metrics
    avg_unit_price,
    min_unit_price,
    max_unit_price,
    
    -- Growth metrics
    previous_month_revenue,
    previous_month_transactions,
    
    CASE
        WHEN previous_month_revenue IS NOT NULL AND previous_month_revenue > 0
        THEN ((monthly_net_revenue - previous_month_revenue) / previous_month_revenue * 100)
        ELSE NULL
    END AS revenue_growth_pct,
    
    CASE
        WHEN previous_month_transactions IS NOT NULL AND previous_month_transactions > 0
        THEN ((transaction_count - previous_month_transactions) / CAST(previous_month_transactions AS FLOAT) * 100)
        ELSE NULL
    END AS transaction_growth_pct,
    
    -- Customer attributes
    customer_tier,
    customer_value_segment,
    
    -- Audit
    CURRENT_TIMESTAMP AS refreshed_at
    
FROM monthly_with_growth