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

estoque_minimo AS (
    SELECT 
        codigo,
        estoque_minimo
    FROM 
        {{ ref('int_estoque_minimo') }}
),

vendas_agrupadas_por_semana AS (
    SELECT
        a.codigo,
        a.referencia,
        year_number,
        iso_week_of_year AS semana_do_ano,
        COUNT(a.referencia) AS contagem_pedidos,
        SUM(a.quantidade) AS quantidade,
        --b.estoque_minimo,
        SUM(a.total) AS total,
        SUM(a.custo_total) AS custo_total
        --CASE WHEN 
          --  b.estoque_minimo < SUM(a.quantidade) THEN 'compras excederam o estoque'
            --ELSE 'OK' END AS observacao_pedidos
    FROM vendas_com_dimensao a
   -- JOIN estoque_minimo b ON a.codigo = b.codigo
    GROUP BY 
        a.codigo,
        a.referencia,
        --b.estoque_minimo,
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
        CASE 
            WHEN COALESCE(ROUND(STDDEV_SAMP(quantidade), 0), 0) = 0 THEN 1
            ELSE ROUND(STDDEV_SAMP(quantidade), 0)
        END AS desvio_padrao
        --estoque_minimo
        --#,
        --#COUNT(select count(distinct(referencia)) from vendas_agrupadas_por_semana where obs_pedido = "compras excederam o estoque" )
    FROM vendas_agrupadas_por_semana 
    GROUP BY
        codigo,
        referencia
       -- estoque_minimo
), 

tabela_frequencia AS (

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
    custo_total,
    ROUND(media + (1.2 * desvio_padrao),0) AS calculo_estoque 

FROM calculo_das_vendas
ORDER BY 
   ranking
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
    frequencia_acumulada,
    freq_relativa_acumulada_percent,    
    CASE WHEN 
        freq_relativa_acumulada_percent <= 50 THEN calculo_estoque * 4
        ELSE calculo_estoque * 3 END AS calculo_estoque,
    faturamento,
    custo_total
FROM tabela_frequencia