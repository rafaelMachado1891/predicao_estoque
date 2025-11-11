WITH vendas AS (
    SELECT 
        *
    FROM {{ ref('int_vendas') }}
),
vendas_agregadas AS (
SELECT 
    codigo,
    referencia,
    COUNT(referencia) AS numero_de_pedidos,
    SUM(quantidade) AS quantidade

FROM vendas
GROUP BY 
    codigo,
    referencia

)
SELECT
    codigo,
    referencia,
    numero_de_pedidos,
    quantidade,
    DENSE_RANK() OVER(ORDER BY numero_de_pedidos DESC)  AS ranking
FROM vendas_agregadas
