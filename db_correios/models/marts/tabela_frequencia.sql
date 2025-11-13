WITH vendas AS (
    SELECT 
        data,
        codigo,
        referencia,
        quantidade,
        total,
        custo_total
    FROM {{ ref('int_vendas') }}
),

vendas_com_dimensao AS (
    SELECT 
        v.codigo,
        v.referencia,
        d.year_number,
        d.iso_week_of_year,     -- ou ISO (segunda a domingo)
        d.week_start_date,
        d.week_end_date,
        v.quantidade,
        v.total,
        v.custo_total
    FROM vendas v
    JOIN {{ ref('dim_date') }} d 
        ON v.data = d.date_day
),

vendas_agrupadas_por_semana AS (
    SELECT
        codigo,
        referencia,
        year_number,
        iso_week_of_year AS semana_do_ano,
        COUNT(referencia) AS contagem_pedidos,
        SUM(quantidade) AS quantidade,
        SUM(total) AS total,
        SUM(custo_total) AS custo_total
    FROM vendas_com_dimensao
    GROUP BY 
        codigo,
        referencia,
        year_number,
        iso_week_of_year
),

calculo_das_vendas AS (
    SELECT 
        codigo,
        referencia,
        SUM(custo_total) AS custo_total,
        SUM(total) AS faturamento,
        SUM(contagem_pedidos) AS frequencia,
        SUM(quantidade) AS quantidade,
        ROUND(AVG(quantidade),0) AS media,
        MIN(quantidade) AS minimo,
        MAX(quantidade) AS maximo,
        ROW_NUMBER() OVER (ORDER BY SUM(contagem_pedidos) DESC) AS ranking,
        ROUND(STDDEV_SAMP(quantidade),0) AS desvio_padrao
    FROM vendas_agrupadas_por_semana 
    GROUP BY
        codigo,
        referencia
)

SELECT 
    codigo,
    referencia,
    frequencia,
    quantidade,
    media,
    minimo,
    maximo,
    ranking,
    desvio_padrao,
    SUM(frequencia) OVER( ORDER BY ranking ) AS frequencia_acumulada,
    ROUND(
            (frequencia * 100.0) / SUM(frequencia) OVER (),
            2
    ) AS freq_relativa_percent,
    ROUND(
            SUM(frequencia) OVER (ORDER BY ranking) * 100.0 / SUM(frequencia) OVER (),
            2
        ) AS freq_relativa_acumulada_percent,
    faturamento,
    custo_total

FROM calculo_das_vendas
ORDER BY 
   ranking
