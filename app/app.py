from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2 
from dotenv import load_dotenv, find_dotenv
import os
import pyodbc
from datetime import datetime
from decimal import Decimal

load_dotenv()

DATA_BASE= os.getenv('DB')
USUARIO= os.getenv('USER')
PASSWORD= quote_plus(os.getenv('PASS'))
HOST_NAME= os.getenv('HOST')

DATA_BASE_URL = f"mssql+pyodbc://{USUARIO}:{PASSWORD}@{HOST_NAME}/{DATA_BASE}?driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(DATA_BASE_URL)

vendas = """
   WITH VENDAS_CORREIOS AS (
	SELECT 
		 A.Numero
		,CONVERT(VARCHAR(10), A.Data_EM, 120) AS Data_EM
		,D.Codigo
		,D.Custo_Medio
		,B.Quantidade
		,b.Preco
		,b.Preco * b.Quantidade as total
		,D.Referencia
		,a.Pedido
		,D.MARCA
		,D.GRUPO
		,YEAR(A.Data_EM)   AS ANO
		,MONTH(A.Data_EM)  AS MES
		,CONCAT(MONTH(A.Data_EM),'-',YEAR(A.Data_EM)) AS [MES-ANO]
		,DATEPART(WEEK,A.Data_EM) AS NUMERO_SEMANA

	FROM NotaS1 A 

	JOIN NotaS2 B
	ON A.Numero = B.Numero

	LEFT JOIN 
		(
			SELECT 
			 A.CODIGO
			,A.Descricao
			,A.Referencia
			,A.Custo_Medio
			,B.descricao AS MARCA
			,C.Descricao AS GRUPO
			FROM Produtos A 
			JOIN 
			marcas B 
			ON B.codigo = A.Marca
			JOIN 
			Grupos C 
			ON C.Codigo = A.Grupo
	) D

	ON B.Codigo = D.Codigo
	WHERE
	a.Codigo_Terceiro = 5865
)


SELECT * FROM VENDAS_CORREIOS
"""

with engine.connect() as connection:
    result = connection.execute(text(vendas))
    
    df = pd.DataFrame(result.fetchall(), columns=result.keys()) 

rename_columns = {'Numero': 'numero', 'Data_EM': 'data', 'Codigo': 'codigo', 'Quantidade': 'quantidade', 'Preco': 'preco', 'Total': 'total', 'Referencia': 'referencia', 'Pedido': 'pedido',
       			  'MARCA': 'linha', 'GRUPO': 'grupo', 'ANO': 'ano', 'MES': 'mes', 'MES-ANO': 'mes_ano', 'NUMERO_SEMANA': 'semana', 'Custo_Medio': 'custo_medio'
            }

df = df.rename(columns=rename_columns,inplace=False)

columns = ['numero', 'data', 'codigo', 'referencia', 'custo_medio', 'quantidade', 'preco', 'total', 'linha', 'grupo']

df = df.astype({
	'numero': int,
	'data':"datetime64[ns]",
	'codigo': str,
	'referencia': str,
 	'custo_medio':float,
	'quantidade': int,
	'preco': float,
	'total': float,
	'linha': str,
	'grupo': str
})

df = df[columns]

estoque_minimo = """
WITH FICHA AS (
SELECT
 A.Codigo							AS CODIGO
,A.Descricao						AS DESCRICAO
,C.descricao                        AS LINHA
,A.Referencia                       AS REFERENCIA
,A.Complemento                      AS COMPLEMENTO
,D.Descricao                        AS GRUPO
,A.Estoque_Minimo                   AS ESTOQUE_MINIMO
,A.Data_Alteracao_Estoque_Min 		AS data_alteracao_estoque
FROM Produtos A 
LEFT JOIN 
Class_Fiscal B ON B.Codigo = A.Classificacao_Fiscal
LEFT JOIN
marcas C ON C.codigo = A.Marca 
LEFT JOIN 
Grupos D
ON A.Grupo = D.Codigo
WHERE
AXEntrada = 3 AND
Situacao = 0  
)

SELECT 
	CODIGO as codigo,
	DESCRICAO as descricao,
	REFERENCIA as referencia,
	ESTOQUE_MINIMO as estoque_minimo,
	LINHA as linha,
	data_alteracao_estoque
FROM Ficha
WHERE REFERENCIA LIKE ('EC%') 

ORDER BY REFERENCIA
"""

with engine.connect() as connection:
    result = connection.execute(text(estoque_minimo))
    
    estoque_minimo = pd.DataFrame(result.fetchall(), columns=result.keys()) 

estoque_minimo = estoque_minimo.astype({
	'codigo': str,
	'descricao': str,
 	'referencia': str,
  	'estoque_minimo': int,
	'linha': str,
    'data_alteracao_estoque': "datetime64[ns]"
})

produtos = """
	WITH FICHA AS (
SELECT
 A.Codigo                                                       AS CODIGO
,A.Descricao                                            AS DESCRICAO
,C.descricao                        AS LINHA
,A.Referencia                       AS REFERENCIA
,A.Complemento                      AS COMPLEMENTO
,D.Descricao                        AS GRUPO
,A.Estoque_Minimo                   AS ESTOQUE_MINIMO
,A.Data_Inclusao					AS data_inclusao
,A.Data_Ultimo_Movimento
FROM Produtos A
LEFT JOIN
Class_Fiscal B ON B.Codigo = A.Classificacao_Fiscal
LEFT JOIN
marcas C ON C.codigo = A.Marca
LEFT JOIN
Grupos D
ON A.Grupo = D.Codigo
WHERE 
AXEntrada = 3 AND
Situacao = 0 AND
Tipo_Produto = 1
)

SELECT
        CODIGO as codigo,
        DESCRICAO as descricao,
        REFERENCIA as referencia,
        LINHA as linha,
		Data_Ultimo_Movimento
FROM Ficha
WHERE Data_Ultimo_Movimento >= DATEFROMPARTS(YEAR(GETDATE()) - 1, 1, 1)
ORDER BY CODIGO
"""

with engine.connect() as connection:
    result = connection.execute(text(produtos))
    
    produtos = pd.DataFrame(result.fetchall(), columns=result.keys()) 

print(produtos)

USER_NAME_POSTGRES= os.getenv("USER_POSTGRES")
PASSWORD_POSTGRES= quote_plus(os.getenv("PASSWORD_POSTGRES"))
HOST_POSTGRE= os.getenv("HOST_POSTGRES")
DB_POSTGRE= os.getenv("DB_POSTGRES")
PORT_POSTGRE = os.getenv("PORT_POSTGRES")

connection_string = f"postgresql://{USER_NAME_POSTGRES}:{PASSWORD_POSTGRES}@{HOST_POSTGRE}:{PORT_POSTGRE}/{DB_POSTGRE}"

target_engine = create_engine(connection_string)

with target_engine.execution_options(isolation_level="AUTOCOMMIT").connect() as connection:
    
    connection.execute(text('DROP TABLE IF EXISTS "vendas" CASCADE'))
    connection.execute(text('DROP TABLE IF EXISTS "estoque_minimo" CASCADE'))
    connection.execute(text('DROP TABLE IF EXISTS "produtos" CASCADE'))
    
df.to_sql(
	name='vendas',
	con=target_engine,
	schema=os.getenv('SCHEMA'),
	if_exists="append",
	index=False
)

estoque_minimo.to_sql(
	name= 'estoque_minimo',
	con=target_engine,
	schema=os.getenv('SCHEMA'),
	if_exists="append",
	index=False
)

produtos.to_sql(
	name= 'produtos',
	con=target_engine,
	schema=os.getenv('SCHEMA'),
	if_exists="append",
	index=False
)

print(produtos)