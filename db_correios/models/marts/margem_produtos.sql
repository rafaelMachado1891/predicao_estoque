
WITH vendas_mes_corrente AS (
    SELECT 
        a.data,
        b.linha,
        a.codigo,
        a.referencia,
        a.custo_medio,
        a.quantidade,
        a.preco,
        a.total AS faturamento,
        a.impostos,
        a.materia_prima

    FROM {{ ref('int_vendas') }} a

    LEFT JOIN {{ ref('int_produtos') }} b
        ON a.codigo = b.codigo
),

resultado_margem AS (
    SELECT 
        linha,
        codigo,
        referencia,
        custo_medio,
        preco,
        quantidade,
        faturamento,
        impostos,
        materia_prima,

        faturamento - impostos - materia_prima AS resultado

    FROM vendas_mes_corrente

    WHERE 
        EXTRACT(MONTH FROM data) = 10
        AND EXTRACT(YEAR FROM data) = 2026
)

SELECT 
    linha,
    codigo,
    referencia,
    preco,
    custo_medio,
    SUM(quantidade) AS quantidade,
    ROUND(SUM(faturamento), 2) AS faturamento,
    ROUND(SUM(impostos), 2) AS impostos,
    ROUND(SUM(materia_prima), 2) AS materia_prima,
    ROUND(SUM(resultado), 2) AS resultado,

    ROUND(
        SUM(resultado) / NULLIF(SUM(faturamento), 0),
        4
    ) AS margem

FROM resultado_margem

GROUP BY
    linha,
    codigo,
    referencia,
    preco,
    custo_medio
