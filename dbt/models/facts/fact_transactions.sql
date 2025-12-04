{{
    config(
        materialized='table',
        tags=['facts', 'transactions']
    )
}}

/*
    Fact Model: Transactions
    ========================
    
    Purpose:
    - Create a transaction fact table with foreign keys to dimensions
    - Store transaction-level metrics
    - Support detailed transaction analysis
    
    Grain: One row per transaction
    
    Note: In production, this could be materialized as incremental for performance
*/

WITH staging_transactions AS (
    SELECT
        transaction_id,
        customer_id,
        product_id,
        transaction_date,
        transaction_month,
        transaction_year,
        transaction_quarter,
        quantity,
        unit_price,
        tax_amount,
        subtotal,
        total_amount,
        is_valid_transaction
    FROM {{ ref('stg_customer_transactions') }}
    WHERE is_valid_transaction = TRUE
),

-- Join with dimension tables to ensure referential integrity
enriched_transactions AS (
    SELECT
        t.transaction_id,
        t.transaction_date,
        t.transaction_month,
        t.transaction_year,
        t.transaction_quarter,
        
        -- Foreign keys to dimensions
        c.customer_key AS customer_fk,
        p.product_key AS product_fk,
        
        -- Transaction measures
        t.quantity,
        t.unit_price,
        t.tax_amount,
        t.subtotal,
        t.total_amount,
        
        -- Derived measures
        t.total_amount - t.tax_amount AS revenue_excluding_tax,
        t.quantity * t.unit_price AS line_item_amount,
        
        -- Denormalized attributes for query performance
        c.customer_tier,
        c.value_segment AS customer_value_segment,
        p.product_name,
        p.price_tier AS product_price_tier,
        p.revenue_segment AS product_revenue_segment
        
    FROM staging_transactions t
    INNER JOIN {{ ref('dim_customers') }} c
        ON t.customer_id = c.customer_id
    INNER JOIN {{ ref('dim_products') }} p
        ON t.product_id = p.product_id
)

SELECT
    -- Primary key
    transaction_id AS transaction_key,
    
    -- Foreign keys
    customer_fk,
    product_fk,
    
    -- Date attributes (for partitioning/filtering)
    transaction_date,
    transaction_month,
    transaction_year,
    transaction_quarter,
    
    -- Additive measures (can be summed)
    quantity AS units_sold,
    subtotal AS gross_amount,
    tax_amount,
    total_amount AS net_amount,
    revenue_excluding_tax,
    
    -- Non-additive measures
    unit_price,
    
    -- Denormalized attributes for performance
    customer_tier,
    customer_value_segment,
    product_name,
    product_price_tier,
    product_revenue_segment,
    
    -- Audit fields
    CURRENT_TIMESTAMP AS created_at
    
FROM enriched_transactions

-- In production, add incremental logic:
-- {% if is_incremental() %}
--   WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
-- {% endif %}