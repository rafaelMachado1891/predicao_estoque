WITH vendas_produtos AS (
    SELECT 
        data,
        codigo,
        referencia,
        custo_medio,
        quantidade,
        preco,
        custo_total,
        total

    FROM 

            {{ ref('int_vendas')}}
)

SELECT 
    codigo,
    referencia,
    custo_medio,
    SUM(quantidade) AS qtde_venda,
    preco,
    SUM(custo_total) AS custo_total,
    SUM(total) AS faturamento,
    SUM(total) - SUM(custo_total) AS margem,
    ROUND((SUM(total) - SUM(custo_total)) / SUM(total),2) AS mrg_percentual

FROM vendas_produtos
WHERE EXTRACT(MONTH FROM data) = 11
GROUP BY 
    codigo,
    referencia,
    custo_medio,
    preco