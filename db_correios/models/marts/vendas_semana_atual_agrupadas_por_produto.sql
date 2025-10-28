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
semana_atual AS  (
	SELECT 
	    date_day AS data,
	    TO_NUMBER(TO_CHAR(date_day, 'IW'), '99') AS semana_ano,
	    TO_NUMBER(TO_CHAR(date_day, 'IYYY'), '9999') AS ano_iso
	FROM  {{ ref('dim_date') }}
	WHERE date_day BETWEEN date_trunc('week', CURRENT_DATE)
                   AND date_trunc('week', CURRENT_DATE) + interval '6 days'
ORDER BY date_day
),
resultado AS (
	SELECT 
		b.data,
		a.codigo,
		a.referencia,
		a.quantidade,
        c.estoque_minimo,
		total
	FROM vendas a 
	JOIN semana_atual b 
	ON a.data = b.data
    JOIN estoque_correios c
    ON a.codigo = c.codigo
),
agrupamento AS (
	SELECT
		codigo,
		referencia,
		SUM(quantidade) AS total_vendido,
        estoque_minimo,
		SUM(total) AS total_faturamento,
		COUNT(referencia) AS numero_pedidos
	FROM resultado
	GROUP BY
		codigo,
		referencia,
        estoque_minimo
)

SELECT 
	
	*
  
FROM agrupamento ORDER BY 5 DESC