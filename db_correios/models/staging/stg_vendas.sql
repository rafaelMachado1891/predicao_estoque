WITH raw_vendas AS (
    SELECT 
        numero ::INTEGER AS numero,
        data::DATE  AS data,
        codigo ::TEXT AS codigo,
        referencia :: TEXT AS referencia,
        custo_medio :: DECIMAL AS custo_medio,
        quantidade ::INTEGER AS quantidade,
        preco :: DECIMAL AS preco
    FROM {{ source ('correios_db', 'vendas') }}
 ),
 resultado AS (
    SELECT 
     data,
     codigo,
     referencia,
     custo_medio,
     quantidade,
     preco,
     quantidade * preco AS total,
     quantidade * custo_medio AS custo_total
    FROM raw_vendas
 )
 SELECT * FROM resultado