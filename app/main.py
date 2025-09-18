import pandas as pd
from datetime import datetime

caminho = "../estoque_correios.csv"

df = pd.read_csv(caminho,sep=",",index_col=False)

df['Data_EM'] = pd.to_datetime(df['Data_EM'], errors='coerce')

df['trimestre'] = df['Data_EM'].dt.quarter
df['dia_semana'] = df['Data_EM'].dt.day_of_week

colunas = ["Numero", "Data_EM", "Codigo", "Descricao", "Quantidade", "Pedido", "MARCA", "GRUPO", "NUMERO_SEMANA", "ANO", "trimestre", "dia_semana"]

df = df[colunas]

rename_columns = {
    "Numero": "numero", "Data_EM": "data", "Codigo": "codigo", "Descricao": "descricao",
    "Quantidade": "quantidade", "Pedido": "pedido", "MARCA": "marca", "GRUPO": "grupo", "NUMERO_SEMANA": "numero_semana", "ANO": "ano"
}

df = df.rename(columns=rename_columns)

df = df.astype({    
    "numero": int,
    "data": "datetime64[ns]",
    "codigo": int,
    "descricao": str,
    "pedido": int,
    "marca": str,
    "grupo": str,
    "numero_semana": int,
    "ano": int, 
    "trimestre": int,
    "dia_semana": int    
})

# dataframe agrupado por dia 

media_diaria = df

media_diaria = media_diaria.groupby(["codigo", "data"],as_index=False).aggregate({"quantidade": "sum",
                                                                                  "descricao": "first",
                                                                                  "marca": "first",
                                                                                  "grupo": "first"
                                                                                  })


media_mensal = df

media_mensal = media_mensal.groupby(["codigo", "ano", "mes"],as_index=False).aggregate({"quantiade": "sum",
                                                                                        "quantidade": "mean"})

print(media_mensal)
# media_diaria = media_diaria.aggregate()
