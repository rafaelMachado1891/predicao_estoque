WITH raw_vendas AS (
    SELECT 
        * 
    FROM {{ source ('correios_db', 'vendas') }}
 )
 SELECT * FROM raw_vendas