WITH vendas AS (
    SELECT 
        data,
        codigo,
        referencia,
        quantidade,
        total
    FROM {{ ref('int_vendas') }}
),
estoque_correios AS (
    SELECT 
        *
    FROM {{ ref('int_estoque_minimo') }}
),
dim_semana AS (
    SELECT 
        date_day AS data,
        TO_NUMBER(TO_CHAR(date_day, 'IW'), '99') AS semana_ano,
        TO_NUMBER(TO_CHAR(date_day, 'IYYY'), '9999') AS ano_iso
    FROM  {{ ref('dim_date') }}
),
resultado AS (
    SELECT 
        d.semana_ano,
        d.ano_iso,
        v.codigo,
        v.referencia,
        v.quantidade,
        e.estoque_minimo,
        v.total
    FROM vendas v
    JOIN dim_semana d ON v.data = d.data
    JOIN estoque_correios e ON v.codigo = e.codigo
),
agrupamento AS (
    SELECT
        semana_ano,
        ano_iso,
        codigo,
        referencia,
        SUM(quantidade) AS total_vendido,
        estoque_minimo,
        SUM(total) AS total_faturamento,
        COUNT(referencia) AS numero_pedidos
    FROM resultado
    GROUP BY
        semana_ano,
        ano_iso,
        codigo,
        referencia,
        estoque_minimo
)

SELECT 
    semana_ano,
    ano_iso,
    codigo,
    referencia,
    total_vendido,
    total_faturamento,
    numero_pedidos,
    estoque_minimo,
    CASE WHEN estoque_minimo < total_vendido THEN 'vendas_excederam_estoque'
         ELSE 'estoque_supriu_as_vendas'
    END AS obs_estoque
FROM agrupamento
ORDER BY ano_iso DESC, semana_ano DESC, numero_pedidos DESC