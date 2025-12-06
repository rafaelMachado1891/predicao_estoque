WITH int_caixas AS (
    SELECT 
        *

    FROM {{ ref('stg_embalagens')}}
),
agregacoes AS (
    SELECT 
        data_movimento, 
        codigo,
        descricao,
        custo_medio,
        quantidade,
        custo_medio * quantidade AS custo_total

    FROM int_caixas
)

SELECT * FROM agregacoes