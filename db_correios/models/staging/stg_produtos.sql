WITH stg_produtos AS (
    SELECT 
        *
    FROM {{ source('correios_db', 'produtos') }}
)

SELECT * FROM stg_produtos