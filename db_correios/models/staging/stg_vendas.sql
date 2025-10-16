WITH raw_vendas AS (
    SELECT 
        numero ::INTEGER AS numero,
        data::DATE  AS data,
        codigo ::TEXT AS codigo,
        referencia :: TEXT AS referencia,
        quantidade ::INTEGER AS quantidade,
        preco :: DECIMAL AS preco
    FROM {{ source ('correios_db', 'vendas') }}
 )
 SELECT * FROM raw_vendas