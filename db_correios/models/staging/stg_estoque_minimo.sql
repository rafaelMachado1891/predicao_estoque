WITH stg_estoque_minimo AS (
    SELECT 
      codigo,
      descricao,
      referencia,
      estoque_minimo,
      linha,
      CASE 
          WHEN data_alteracao_estoque < CURRENT_DATE - INTERVAL '2 years'
          THEN NULL 
          ELSE data_alteracao_estoque
      END AS data_alteracao_estoque
    FROM {{ source('correios_db', 'estoque_minimo') }}
)
SELECT 
  codigo,
  descricao,
  referencia,
  estoque_minimo,
  linha,
  data_alteracao_estoque
FROM stg_estoque_minimo