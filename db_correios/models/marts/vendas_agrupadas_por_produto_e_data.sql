WITH vendas_agrupadas_por_data AS (    
    SELECT 
        data,
        codigo,
        referencia,
        SUM(quantidade) AS quantidade,
        SUM(quantidade * preco) AS total
    FROM {{ ref('int_vendas') }}
    GROUP BY 
        data,
        codigo,
        referencia
    ORDER BY 1,2 
)
SELECT * FROM vendas_agrupadas_por_data