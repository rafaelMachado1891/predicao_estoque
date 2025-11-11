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
    va.codigo,
    va.referencia,
    va.numero_de_pedidos,
    va.quantidade,
    DENSE_RANK() OVER (ORDER BY va.numero_de_pedidos DESC) AS ranking
FROM vendas_agregadas va
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('int_estoque_minimo') }} est
    WHERE est.codigo = va.codigo
)
