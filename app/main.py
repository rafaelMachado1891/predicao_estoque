import pandas as pd
from pandas.tseries.offsets import DateOffset
from datetime import datetime

caminho = "../estoque_correios.csv"

df = pd.read_csv(caminho,sep=",",index_col=False)

df['Data_EM'] = pd.to_datetime(df['Data_EM'], errors='coerce')

df['trimestre'] = df['Data_EM'].dt.quarter
df['dia_semana'] = df['Data_EM'].dt.day_of_week
df['semestre'] = df["MES"].apply(lambda m: 1 if m <=6  else 2)

colunas = ["Numero", "Data_EM", "Codigo", "Descricao", "Quantidade", "Pedido", "MARCA", "GRUPO", "NUMERO_SEMANA", "ANO", "trimestre", "dia_semana", "MES", "MES-ANO", "semestre", "Referencia"]

df = df[colunas]

rename_columns = {
    "Numero": "numero", "Data_EM": "data", "Codigo": "codigo", "Descricao": "descricao", "Referencia": "referencia",
    "Quantidade": "quantidade", "Pedido": "pedido", "MARCA": "marca", "GRUPO": "grupo", "NUMERO_SEMANA": "numero_semana", "ANO": "ano","MES-ANO": "mes-ano","MES": "mes"
}

df = df.rename(columns=rename_columns)

df["referencia"] = df["referencia"].str.replace("EC ", "", regex=False)
df["referencia"] = df["referencia"].str.replace("8136 PR", "136 PR", regex=False)
df["referencia"] = df["referencia"].str.replace("81060 DOU/CRU NATU", "81060 DOURADO/CRU NATU", regex=False)

df = df.astype({    
    "numero": int,
    "data": "datetime64[ns]",
    "codigo": int,
    "descricao": str,
    'referencia': str,
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

consumo_semanal = df[df["data"] >= ultima_semana].groupby(["referencia"],as_index=False).agg(quantidade_semana=("quantidade", "sum"))                                                                                                                                                                                                                                                          
                                                                                                
consumo_mensal = df[df["data"] >= ultimo_mes].groupby(["referencia"],as_index=False).agg(quantidade_mes=("quantidade", "sum"))

consumo_trimestre = df[df["data"] >= ultimo_trimestre].groupby(["referencia"], as_index=False).agg(quantidade_trimestre=("quantidade", "sum"))

consumo_semestre = df[df["data"] >= ultimo_semestre].groupby(["referencia"], as_index=False).agg(quantidade_semestre=("quantidade", "sum"))                                                                                                          

consumo_geral = df.groupby(["referencia", "numero_semana", "ano"], as_index=False).agg(quantidade_geral=("quantidade", "sum"),
                                                                                                frequencia_geral=("referencia", "count")
                                                                                                )
consumo_geral = consumo_geral.groupby(["referencia"], as_index=False).agg(quantidade_geral=("quantidade_geral", "sum"),
                                                                                   media_semana=("quantidade_geral", "mean"),
                                                                                   desvio_semana=("quantidade_geral", "std"),
                                                                                   frequencia_geral=("frequencia_geral","sum")
                                                                                   )                                                                                         

consumo_geral["media_semana"] = consumo_geral["media_semana"].round(0)
consumo_geral["desvio_semana"] = consumo_geral["desvio_semana"].round(0)

consumo_geral["estoque_ideal"] = (consumo_geral["media_semana"] + consumo_geral["desvio_semana"] * 1.5).round()

consumo_geral["rank_geral"] = consumo_geral["quantidade_geral"].rank(ascending=False, method="dense").astype(int)

consumo_semanal["rank_semana"] = consumo_semanal["quantidade_semana"].rank(ascending=False, method="dense").astype(int)

consumo_trimestre["rank_trim"] = consumo_trimestre["quantidade_trimestre"].rank(ascending=False, method="dense").astype(int)

consumo_semestre["rank_semestre"] = consumo_semestre["quantidade_semestre"].rank(ascending=False, method="dense").astype(int)

consumo_mensal["rank_mensal"] = consumo_mensal["quantidade_mes"].rank(ascending=False, method="dense").astype(int)


relatorio = consumo_geral.merge(consumo_mensal[["referencia", "rank_mensal", "quantidade_mes"]], on="referencia", how="left")
relatorio = relatorio.merge(consumo_trimestre[["referencia", "rank_trim", "quantidade_trimestre"]], on="referencia", how="left")
relatorio = relatorio.merge(consumo_semestre[["referencia", "rank_semestre", "quantidade_semestre"]], on="referencia", how="left")
relatorio = relatorio.merge(consumo_semanal[["referencia", "rank_semana", "quantidade_semana"]], on="referencia", how="left")

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

relatorio["tendencia_semana_vs_mensal"] = relatorio.apply(lambda r: classificar_tendencia(r, "rank_semana", "rank_mensal"), axis=1)
relatorio["tendencia_mes_vs_trim"] = relatorio.apply(lambda r: classificar_tendencia(r, "rank_mensal", "rank_trim"), axis=1)
relatorio["tendencia_trim_vs_semestre"] = relatorio.apply(lambda r: classificar_tendencia(r, "rank_trim", "rank_semestre"), axis=1 )
relatorio["tendencia_trimestre_vs_geral"] = relatorio.apply(lambda r: classificar_tendencia( r, "rank_trim", "rank_geral"), axis=1)
relatorio["tendencia_semestre_vs_geral"] = relatorio.apply(lambda r: classificar_tendencia(r, "rank_semestre", "rank_geral"), axis=1 )

# relatorio de frequencia de vendas

frequencia = df.copy()

frequencia = frequencia.groupby(["referencia"], as_index= False).agg(frequencia=("referencia", "count"))
frequencia['frequencia_relativa'] = frequencia["frequencia"] / (frequencia["frequencia"].sum())
frequencia= frequencia.sort_values(by="frequencia", ascending= False). reset_index(drop=True)
frequencia["acumulada"] = frequencia["frequencia"].cumsum()
frequencia["relativa_acumulada"] = frequencia["frequencia_relativa"].cumsum()

frequencia = frequencia.merge(relatorio[
        ["referencia",'quantidade_geral', 'media_semana', 'desvio_semana',
        'frequencia_geral', 'estoque_ideal', 'rank_geral', 
        'rank_semana', 'quantidade_semana',
        'tendencia_semana_vs_mensal', 'rank_mensal', 'quantidade_mes',
        'tendencia_mes_vs_trim', 'rank_trim', 'quantidade_trimestre', 'tendencia_trim_vs_semestre',
        'rank_semestre','quantidade_semestre', 'tendencia_semestre_vs_geral',
        'tendencia_trimestre_vs_geral']
], on="referencia", how="left")

marca = df[["referencia", "marca"]].drop_duplicates()

frequencia = frequencia.merge(marca[["referencia", "marca"]],on="referencia", how="left")

frequencia.to_excel("relatorio_frequencia.xlsx", index=False)

print(f'relatorio gerado com sucesso!')

embalagens = df.copy()

embalagens = embalagens[embalagens["codigo"] == 10827]

embalagens = embalagens.groupby(["codigo","descricao","pedido","data", "quantidade"], as_index=False).agg(contagem=("descricao", "count"))

print(embalagens.head(50))