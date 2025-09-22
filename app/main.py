import pandas as pd
from pandas.tseries.offsets import DateOffset
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

last_row = df["data"].loc[df['data'].idxmax()]
ultima_data = df["data"].max()
ultima_semana = ultima_data - DateOffset(weeks=1)
ultimo_ano = ultima_data - DateOffset(months=12)
ultimo_trimestre = ultima_data - DateOffset(months=3)
ultimo_semestre = ultima_data - DateOffset(months=6)
ultimo_mes = ultima_data - DateOffset(months=1)

consumo_semanal = df[df["data"] >= ultima_semana].groupby(["codigo", "descricao"],as_index=False).agg(quantidade_semana=("quantidade", "sum"))                                                                                                                                                                                                                                                          
                                                                                                
consumo_mensal = df[df["data"] >= ultimo_mes].groupby(["codigo", "descricao"],as_index=False).agg(quantidade_mes=("quantidade", "sum"))

consumo_trimestre = df[df["data"] >= ultimo_trimestre].groupby(["codigo", "descricao"], as_index=False).agg(quantidade_trimestre=("quantidade", "sum"))

consumo_semestre = df[df["data"] >= ultimo_semestre].groupby(["codigo", "descricao"], as_index=False).agg(quantidade_semestre=("quantidade", "sum"))                                                                                                          

consumo_geral = df.groupby(["codigo", "descricao", "numero_semana", "ano"], as_index=False).agg(quantidade_geral=("quantidade", "sum"),
                                                                                                frequencia_geral=("descricao", "count")
                                                                                                )
consumo_geral = consumo_geral.groupby(["codigo", "descricao"], as_index=False).agg(quantidade_geral=("quantidade_geral", "sum"),
                                                                                   media_semana=("quantidade_geral", "mean"),
                                                                                   desvio_semana=("quantidade_geral", "std"),
                                                                                   frequencia_geral=("frequencia_geral","sum")
                                                                                   )                                                                                         

consumo_geral["estoque_ideal"] = (consumo_geral["media_semana"] + consumo_geral["desvio_semana"] * 1.5).round()

consumo_geral["rank_geral"] = consumo_geral["quantidade_geral"].rank(ascending=False, method="dense").astype(int)

consumo_semanal["rank_semana"] = consumo_semanal["quantidade_semana"].rank(ascending=False, method="dense").astype(int)

consumo_trimestre["rank_trim"] = consumo_trimestre["quantidade_trimestre"].rank(ascending=False, method="dense").astype(int)

consumo_semestre["rank_semestre"] = consumo_semestre["quantidade_semestre"].rank(ascending=False, method="dense").astype(int)

consumo_mensal["rank_mensal"] = consumo_mensal["quantidade_mes"].rank(ascending=False, method="dense").astype(int)


relatorio = consumo_geral.merge(consumo_mensal[["codigo", "rank_mensal"]], on="codigo", how="left")
relatorio = relatorio.merge(consumo_trimestre[["codigo", "rank_trim"]], on="codigo", how="left")
relatorio = relatorio.merge(consumo_semestre[["codigo", "rank_semestre"]], on="codigo", how="left")
relatorio = relatorio.merge(consumo_semanal[["codigo", "rank_semana", "quantidade_semana"]], on="codigo", how="left")

# relatorio.fillna(0, method=None, inplace= True)

def classificar_tendencia(row, col1, col2):
    if pd.isna(row[col1]) or pd.isna(row[col2]):
        return "sem dado"
    if row[col1] < row[col2]:
        return "acsendente"
    elif row[col1] > row[col2]:
        return "descendente"
    else: 
        return "estavel"

relatorio["tendencia_mes_vs_trim"] = relatorio.apply(lambda r: classificar_tendencia(r, "rank_mensal", "rank_trim"), axis=1)
relatorio["tendencia_trim_vs_semestre"] = relatorio.apply(lambda r: classificar_tendencia(r, "rank_trim", "rank_semestre"), axis=1 )
relatorio["tendencia_semana_vs_mensal"] = relatorio.apply(lambda r: classificar_tendencia(r, "rank_semana", "rank_mensal"), axis=1)
relatorio["tendencia_trimestre_vs_geral"] = relatorio.apply(lambda r: classificar_tendencia( r, "rank_trim", "rank_geral"), axis=1)
relatorio["tendencia_semestre_vs_geral"] = relatorio.apply(lambda r: classificar_tendencia(r, "rank_semestre", "rank_geral"), axis=1 )

relatorio.to_excel("relatorio.xlsx", index=False)

