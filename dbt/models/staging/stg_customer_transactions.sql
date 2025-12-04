{{
    config(
        materialized='view',
        tags=['staging', 'customer-transactions']
    )
}}

/*
    Staging Model: Customer Transactions
    ====================================

    Purpose:
    - Create a standardized view of cleaned transaction data
    - Add derived fields and business logic
    - Prepare data for dimensional modeling

    Source: cleaned_data.customer_transactions
*/

WITH source_data AS (
    SELECT *
    FROM {{ source('cleaned_data', 'customer_transactions') }}
),

enhanced_data AS (
    SELECT
        -- keys
        transaction_id,
        customer_id,
        product_id,

        -- Dates
        transaction_date,
        DATE_TRUNC('month', transaction_date)::DATE AS transaction_month,
        EXTRACT(YEAR FROM transaction_date)::INTEGER AS transaction_year,
        EXTRACT(QUARTER FROM transaction_date)::INTEGER AS transaction_quarter,
        EXTRACT(MONTH FROM transaction_date)::INTEGER AS month_number,
        TO_CHAR(transaction_date, 'Month') AS month_name,
        TO_CHAR(transaction_date, 'Day') AS day_name,

        -- Product information
        product_name,

        -- Transaction details (already cleaned)
        quantity,
        price AS unit_price,
        tax AS tax_amount,

        -- Calculated fields
        quantity * price AS subtotal,
        (quantity * price) + tax AS total_amount,

        -- Business flags
        CASE
            WHEN quantity IS NULL OR quantity <= 0 THEN FALSE
            ELSE TRUE
        END AS is_valid_quantity,

        CASE
            WHEN price IS NULL OR price <= 0 THEN FALSE
            ELSE TRUE
        END AS is_valid_price,

        CASE
            WHEN customer_id IS NOT NULL
                AND quantity IS NOT NULL AND quantity > 0
                AND price IS NOT NULL AND price > 0
            THEN TRUE
            ELSE FALSE
        END AS is_valid_transaction,

        -- Audit fields
        loaded_at,
        CURRENT_TIMESTAMP AS transformed_at

    FROM source_data
)

SELECT * FROM enhanced_data
