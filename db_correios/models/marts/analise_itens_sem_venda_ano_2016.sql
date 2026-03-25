WITH  vendas_2026 AS (
    SELECT 
        codigo,
        referencia,
        SUM(quantidade) AS quantidade,
        COUNT(referencia) AS contagem_pedidos
    FROM 
        {{  ref('int_vendas') }} a
    WHERE EXTRACT(YEAR FROM data) = 2026 
    GROUP BY 
        codigo,
        referencia
        ),

    estoque_minimo AS (

        SELECT 
            * 
        FROM 
            
            {{ ref('int_estoque_minimo') }} a
    ),

    estoque_atual AS (
        SELECT 
            codigo,
            data,
            saldo_estoque
        FROM 
            {{ ref('int_saldo_estoque') }} 
    )

    SELECT 
        a.codigo,
        a.referencia,
        a.estoque_minimo,       
        c.saldo_estoque
    
    FROM estoque_minimo a
    LEFT JOIN 
        vendas_2026 b 
    ON 
        a.codigo = b.codigo
    LEFT JOIN 
        estoque_atual c
    ON
        a.codigo = c.codigo
    WHERE b.codigo IS NULL  