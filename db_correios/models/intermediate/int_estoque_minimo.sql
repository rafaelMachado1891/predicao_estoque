WITH int_estoque_minimo AS (
    SELECT 
        *
    FROM {{ ref('stg_estoque_minimo') }}

)
SELECT 
    *
FROM int_estoque_minimo