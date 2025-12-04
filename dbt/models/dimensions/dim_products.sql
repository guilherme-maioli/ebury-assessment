{{
    config(
        materialized='table',
        tags=['dimensions', 'products']
    )
}}

/*
    Dimension Model: Products
    =========================
    
    Purpose:
    - Create a product dimension table with SCD Type 1
    - Aggregate product-level metrics
    - Support product analysis and inventory management
    
    Grain: One row per product
*/

WITH product_transactions AS (
    SELECT
        product_id,
        product_name,
        transaction_date,
        quantity,
        unit_price,
        total_amount,
        is_valid_transaction
    FROM {{ ref('stg_customer_transactions') }}
    WHERE is_valid_transaction = TRUE
),

product_metrics AS (
    SELECT
        product_id,
        -- Use the most recent product name (in case of variations)
        MAX(product_name) AS product_name,
        
        -- Transaction metrics
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT transaction_date) AS unique_transaction_days,
        
        -- Date ranges
        MIN(transaction_date) AS first_sale_date,
        MAX(transaction_date) AS last_sale_date,
        
        -- Sales metrics
        SUM(quantity) AS total_units_sold,
        SUM(total_amount) AS total_revenue,
        
        -- Price analytics
        AVG(unit_price) AS avg_unit_price,
        MIN(unit_price) AS min_unit_price,
        MAX(unit_price) AS max_unit_price,
        STDDEV(unit_price) AS stddev_unit_price,
        
        -- Quantity analytics
        AVG(quantity) AS avg_quantity_per_transaction,
        MIN(quantity) AS min_quantity_per_transaction,
        MAX(quantity) AS max_quantity_per_transaction,
        
        -- Customer reach
        COUNT(DISTINCT customer_id) AS unique_customers
        
    FROM {{ ref('stg_customer_transactions') }}
    WHERE is_valid_transaction = TRUE
    GROUP BY product_id
),

product_segments AS (
    SELECT
        *,
        
        -- Product performance segmentation
        CASE
            WHEN total_revenue >= 50000 THEN 'Star Product'
            WHEN total_revenue >= 20000 THEN 'High Performer'
            WHEN total_revenue >= 5000 THEN 'Medium Performer'
            ELSE 'Low Performer'
        END AS revenue_segment,
        
        CASE
            WHEN total_units_sold >= 1000 THEN 'High Volume'
            WHEN total_units_sold >= 500 THEN 'Medium Volume'
            ELSE 'Low Volume'
        END AS volume_segment,
        
        -- Price positioning
        CASE
            WHEN avg_unit_price >= 100 THEN 'Premium'
            WHEN avg_unit_price >= 50 THEN 'Mid-Range'
            ELSE 'Value'
        END AS price_tier,
        
        -- Recency
        CURRENT_DATE - last_sale_date AS days_since_last_sale,
        
        -- Revenue per customer
        CASE
            WHEN unique_customers > 0 THEN total_revenue / unique_customers
            ELSE 0
        END AS revenue_per_customer
        
    FROM product_metrics
)

SELECT
    -- Primary key
    product_id AS product_key,
    
    -- Product attributes
    product_id,
    product_name,
    
    -- Transaction metrics
    total_transactions,
    unique_transaction_days,
    unique_customers,
    
    -- Date information
    first_sale_date,
    last_sale_date,
    days_since_last_sale,
    
    -- Sales metrics
    total_units_sold,
    total_revenue,
    revenue_per_customer,
    
    -- Price analytics
    avg_unit_price,
    min_unit_price,
    max_unit_price,
    stddev_unit_price,
    price_tier,
    
    -- Quantity analytics
    avg_quantity_per_transaction,
    min_quantity_per_transaction,
    max_quantity_per_transaction,
    
    -- Segmentation
    revenue_segment,
    volume_segment,
    
    -- Audit fields
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM product_segments