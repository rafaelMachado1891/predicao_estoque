WITH int_produtos AS (
    SELECT
        *
    FROM {{ ref('stg_produtos') }}
)
SELECT * FROM int_produtos