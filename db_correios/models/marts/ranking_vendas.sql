WITH vendas AS (
    SELECT 
        data,
        COUNT(referencia) AS volumes,
        SUM(total) total_faturado,
        SUM(custo_total) AS custo,
        40.00 AS frete

    FROM {{ ref('int_vendas') }}
    GROUP BY data
)
select 
    data,
    volumes,
    total_faturado,
    frete,
    ROUND(frete / total_faturado, 4)  as perc_frete,
    custo,
    (total_faturado - custo - frete ) AS margem,
    ROUND((total_faturado - custo - frete ) / total_faturado, 4) as margem_percentual



from vendas