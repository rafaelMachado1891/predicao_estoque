WITH tbl_dim_date AS (
    SELECT 
        date_day AS data,
        month_of_year AS mes,
        month_name AS mes_name_short

    FROM 

        {{ ref('dim_date')}}

),
vendas AS (
    SELECT 
        b.data AS data_venda,
        COUNT(referencia) AS volumes,
        SUM(total) total_faturado,
        SUM(custo_total) AS custo,
        40.00 AS frete

    FROM {{ ref('int_vendas') }} a
    JOIN tbl_dim_date b ON a.data = b.data
    GROUP BY  b.data
),
embalagens AS (
    SELECT 
        b.data AS data_saida,
        SUM(custo_total) AS custo_total
    FROM {{ ref('int_embalagens') }} a
    JOIN tbl_dim_date b ON a.data_movimento = b.data

    GROUP BY b.data
)
SELECT
    a.data_venda,
    a.volumes,
    a.total_faturado,
    frete,
    ROUND(frete / total_faturado, 4)  as perc_frete,
    custo,
    COALESCE(b.custo_total, 0)  AS despesas_com_embalagem,
    (total_faturado - (custo + frete + COALESCE(b.custo_total,0)) ) AS margem,
    ROUND((total_faturado - (custo + frete + COALESCE(b.custo_total,0))) / total_faturado, 4) as margem_percentual
FROM vendas a
LEFT JOIN embalagens b  
ON a.data_venda = b.data_saida