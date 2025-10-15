WITH stg_estoque_minimo AS (
    SELECT 
      *
    FROM {{ source('correios_db', 'estoque_minimo') }}
)
SELECT * FROM stg_estoque_minimo