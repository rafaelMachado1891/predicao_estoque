WITH int_saldo_estoque AS (
    SELECT 
        codigo,
        data,
        saldo_estoque
    FROM {{ ref('stg_saldo_estoque') }}
)

SELECT * FROM int_saldo_estoque