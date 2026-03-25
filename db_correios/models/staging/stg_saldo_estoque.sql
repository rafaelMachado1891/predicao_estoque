WITH raw_saldo_estoque AS (
    SELECT 
        codigo::TEXT AS codigo,
        data::DATE AS data,
        REPLACE(saldo, '.0000', '')::INTEGER AS saldo_estoque
    FROM {{ source ('correios_db', 'saldo_estoque') }}
),

resultado AS (
    SELECT 
        codigo,
        data,
        saldo_estoque
    FROM raw_saldo_estoque
)  

SELECT * FROM resultado