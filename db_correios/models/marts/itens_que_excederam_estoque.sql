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
estoque_minimo AS (
    SELECT 
        codigo,
        descricao,
        estoque_minimo
    FROM {{ ref('int_estoque_minimo') }}

),
resultado AS (
    SELECT
     a.data,
     a.codigo,
     a.referencia,
     a.quantidade,
     a.total,
     COUNT(a.referencia) AS qtde_pedidos,
     b.estoque_minimo
    FROM vendas_agrupadas_por_data a
    JOIN 
    estoque_minimo b ON a.codigo = b.codigo
    GROUP BY 
     a.data,
     a.codigo,
     a.referencia,
     a.quantidade,
     a.total,
     b.estoque_minimo
),
agregado AS (
    SELECT 
        data,
        codigo,
        referencia,
        quantidade,
        total,
        qtde_pedidos,
        estoque_minimo,
        CASE
            WHEN quantidade > estoque_minimo THEN 'quantidade excedeu o estoque minimo'
            ELSE 'estoque supriu a demanda'
        END AS obs_pedidos
    FROM resultado
)
SELECT 
    data,
    codigo,
    referencia,
    quantidade,
    total,
    qtde_pedidos,
    estoque_minimo,
    obs_pedidos
FROM agregado
WHERE obs_pedidos = 'quantidade excedeu o estoque minimo'