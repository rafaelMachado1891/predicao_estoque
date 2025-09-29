import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2 
from dotenv import load_dotenv
import os
import pyodbc

load_dotenv()

DATA_BASE=os.getenv('DB')
USUARIO=os.getenv('USER')
PASSWORD=os.getenv('PASS')
HOST_NAME=os.getenv('HOST')

DATA_BASE_URL = f"mssql+pyodbc://{USUARIO}:{PASSWORD}@{HOST_NAME}/{DATA_BASE}?driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(DATA_BASE_URL)

query = """
   WITH VENDAS_CORREIOS AS (
	SELECT 
		 A.Numero
		,CONVERT(VARCHAR(10), A.Data_EM, 120) AS Data_EM
		,D.Codigo
		,B.Quantidade
		,B.Descricao
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

	JOIN 
		(
			SELECT 
			 A.CODIGO
			,A.Descricao
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
    result = connection.execute(text(query))
    
    df = pd.DataFrame(result.fetchall(), columns=result.keys()) 


print(df)