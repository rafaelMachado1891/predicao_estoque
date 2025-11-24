{% snapshot store_estoque_produtos %}

    {{
        config(
            target_schema='stg_snapshot',
            unique_key='codigo',
            strategy='check',
            check_cols=['codigo', 'estoque_minimo']
        )
    }}

    SELECT 
     codigo,
     descricao,
     referencia,
     estoque_minimo,
     linha
   
    FROM {{ ref('stg_estoque_minimo')}}

    {% endsnapshot %}