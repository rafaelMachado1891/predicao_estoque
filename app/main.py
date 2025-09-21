import pandas as pd
from datetime import datetime

caminho = "../estoque_correios.csv"

df = pd.read_csv(caminho,sep=",",index_col=False)

df['Data_EM'] = pd.to_datetime(df['Data_EM'], errors='coerce')

df['trimestre'] = df['Data_EM'].dt.quarter
df['dia_semana'] = df['Data_EM'].dt.day_of_week
df['semestre'] = df["MES"].apply(lambda m: 1 if m <=6  else 2)

colunas = ["Numero", "Data_EM", "Codigo", "Descricao", "Quantidade", "Pedido", "MARCA", "GRUPO", "NUMERO_SEMANA", "ANO", "trimestre", "dia_semana", "MES", "MES-ANO", "semestre"]

df = df[colunas]

rename_columns = {
    "Numero": "numero", "Data_EM": "data", "Codigo": "codigo", "Descricao": "descricao",
    "Quantidade": "quantidade", "Pedido": "pedido", "MARCA": "marca", "GRUPO": "grupo", "NUMERO_SEMANA": "numero_semana", "ANO": "ano","MES-ANO": "mes-ano","MES": "mes" 
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
    "mes": int,
    "mes-ano": str,
    "ano": int, 
    "trimestre": int,
    "dia_semana": int,
    "semestre": int
})

consumo_semanal = df.groupby(["codigo", "descricao","ano","numero_semana" ],as_index=False).agg(quantidade_semana=("quantidade", "sum"))                                                                                     
                                                                                                
consumo_mensal = df.groupby(["codigo", "descricao", "mes-ano"],as_index=False).agg(quantidade_mes=("quantidade", "sum"))

consumo_trimestre = df.groupby(["codigo", "descricao", "trimestre", "ano"], as_index=False).agg(quantidade_trimestre=("quantidade", "sum"))

consumo_semestre = df.groupby(["codigo", "descricao", "ano", "semestre"], as_index=False).agg(quantidade_semestre=("quantidade", "sum"))

consumo_geral = df.groupby(["codigo", "descricao"], as_index=False).agg(quantidade=("quantidade", "sum"), 
                                                                        media_geral=("quantidade", "mean")
                                                                    )

estoque_ideal = consumo_semanal.groupby(["codigo", "descricao"], as_index=False).agg(quantidade=("quantidade_semana", "sum"),
                                                                                     media_semana=("quantidade_semana","mean"),
                                                                                     desvio_semana=("quantidade_semana","std")
                                                                                     )

estoque_ideal["estoque_ideal"] = (estoque_ideal["media_semana"] + estoque_ideal["desvio_semana"] * 1.5).round()

last_row = df.loc[df['data'].idxmax()]
ultima_semana = int(last_row['numero_semana'])
ultimo_ano = df["ano"].max()
ultimo_trimestre = int(last_row["trimestre"])
ultimo_semestre = int(last_row["semestre"])
ultimo_mes = int(last_row["mes"])

rank_atual = consumo_semanal[(consumo_semanal["ano"] == ultimo_ano) & 
                             (consumo_semanal["numero_semana"] == ultima_semana)
                             ].copy()
rank_atual["rank_atual"] = rank_atual["quantidade_semana"].rank(ascending=False, method="dense").astype(int)


rank_trim = consumo_trimestre[
    (consumo_trimestre["ano"] == ultimo_ano) &
    (consumo_trimestre["trimestre"] == ultimo_trimestre)
].copy()
rank_trim["rank_trim"] = rank_trim["quantidade_trimestre"].rank(ascending=False, method="dense").astype(int)

rank_semestre = consumo_semestre[
    (consumo_semestre["ano"] == ultimo_ano) &
    (consumo_semestre["semestre"] == ultimo_semestre)
].copy()
rank_semestre["rank_semestre"] = rank_semestre["quantidade_semestre"].rank(ascending=False, method="dense").astype(int)

print(rank_semestre)

# media_diaria = media_diaria.aggregate()
