WITH vendas AS (
	SELECT 
		data,
		codigo,
		referencia,
		quantidade,
		total
	FROM {{ ref('int_vendas') }}
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
		codigo,
		referencia,
		quantidade,
		total
	FROM vendas a 
	JOIN semana_atual b 
	ON a.data = b.data
)

SELECT 
  *
FROM resultado ORDER BY 3 