WITH stg_embalagens AS (
    SELECT 
        "codigo",
        "Descricao",
        "custo_medio",
        "Data_Movimento",
        "Qt_Saida"
    FROM  {{ source('correios_db', 'embalagens_correios')}}
),
tabela_tratada AS (
    SELECT 
        "codigo":: TEXT AS codigo,
        "Descricao":: TEXT AS descricao,
        "custo_medio":: DECIMAL AS custo_medio,
        "Data_Movimento":: DATE AS data_movimento,
        "Qt_Saida":: INTEGER AS quantidade
    FROM stg_embalagens
)
SELECT 


* FROM tabela_tratada