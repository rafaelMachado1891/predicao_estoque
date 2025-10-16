WITH int_vendas AS (
    SELECT 
        *
    FROM {{ ref('stg_vendas') }}
)
SELECT * FROM int_vendas