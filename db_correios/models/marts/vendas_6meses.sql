WITH vendas_6_meses AS (
	SELECT 
		data,
		codigo,
		referencia,
		quantidade
	FROM 
        {{ ref('int_vendas') }}
	WHERE
		data >= CURRENT_DATE - INTERVAL '4 months'
),
vendas_agrupadas_por_data AS (
	SELECT
		data,
		codigo,
		referencia,
		SUM(quantidade) AS quantidade,
		COUNT(data) AS contagem_pedidos

	FROM 
		vendas_6_meses	
	GROUP BY 
		data,
		codigo,
		referencia
),

vendas_agrupadas AS (
		SELECT
			codigo,
			referencia,
			SUM(quantidade) AS quantidade,
			COUNT(referencia) AS contagem_pedidos,
			COUNT(DISTINCT (data)) AS contagem_data,
			ROUND(
				SUM(quantidade) / COUNT(DISTINCT(data))
			,2) AS media,
			MAX(quantidade) AS maximo,
			ROUND(
				STDDEV(quantidade), 2 
			)	AS desvio_padrao
		FROM 
			vendas_6_meses	
		GROUP BY 
			codigo,
			referencia
),
ranking_vendas AS (
	SELECT 
		codigo,
		referencia,
		quantidade,
		contagem_pedidos,
		contagem_data,
		maximo,
		media,
		desvio_padrao,
		ROW_NUMBER() OVER (ORDER BY contagem_pedidos DESC ) AS ranking,
		ROUND(((media) + (desvio_padrao * 1.20))*5,0) AS calculo_estoque
	FROM vendas_agrupadas
)

SELECT a.*, b.estoque_minimo FROM ranking_vendas a LEFT JOIN {{ ref('int_estoque_minimo') }} b ON a.codigo = b.codigo
