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
),
vendas_agrupada_por_produto AS (
    SELECT 
        codigo,
        referencia,
        SUM(quantidade) AS quantidade,
        SUM(total) AS total,
        COUNT(codigo) AS qtd_pedidos
    FROM vendas_agrupadas_por_data
    GROUP BY 
        codigo,
        referencia
)
SELECT * FROM vendas_agrupada_por_produto