WITH int_vendas AS (
    SELECT 
        data,
        codigo,
        referencia,
        custo_medio,
        quantidade,
        preco,
        total,
        custo_total
    FROM {{ ref('stg_vendas') }}
),
estoque_minimo AS (
    SELECT 
        codigo,
        estoque_minimo,
        linha,
        dbt_valid_from,
        dbt_valid_to
    FROM 
        {{ ref('store_estoque_produtos') }}

),

resultado AS (

    SELECT
        a.data,
        a.codigo,
        a.referencia,
        a.custo_medio,
        a.quantidade,
        a.preco,
        a.total,
        (a.total * (0.12 + 0.03+ 0.0065 + 0.0108+ 0.018 ) )+ a.custo_total AS custo_total,
        b.estoque_minimo
    FROM int_vendas a
    LEFT JOIN estoque_minimo b 
    ON a.codigo = b.codigo
    AND a.data >= b.dbt_valid_from
    AND (a.data < b.dbt_valid_to OR b.dbt_valid_to IS NULL)
)

SELECT * FROM resultado