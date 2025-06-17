import pandas as pd
from datetime import date

caminho = pd.read_csv("../estoque_correios.csv", sep=";")

df = pd.DataFrame(caminho)

colunas = ["Numero", "Data_EM", "Codigo", "Descricao","Quantidade", "Pedido", "MARCA", "GRUPO"]
df = df[colunas]

rename_columns = {"Numero": "numero", "Data_EM": "data","Codigo": "codigo","Descricao":"descricao", "Quantidade": "quantidade",
                  "Pedido": "pedido", "MARCA": "marca", "GRUPO":"grupo"}

df.rename(columns=rename_columns, inplace=True)

tipo_dados = {
    "numero": int,
    "data": object,
    "codigo": str,
    "descricao": str,
    "quantidade": int,
    "pedido": int,
    "marca": str,
    "grupo": str
}

df = df.astype(tipo_dados)

df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")

data_consulta = input("a partir de qual data você deseja consultar? digite no formato(ano/mes/dia): ")

data_consulta = pd.to_datetime(data_consulta, format="%Y/%m/%d", errors="coerce")

df = df.loc[df["data"]>= data_consulta]

df["descricao"] = df["descricao"].str.rstrip()

agregate = df.groupby(by=["codigo", "descricao", "quantidade"], as_index=False).agg(
    quantidade_sum=("quantidade", "sum"),
    quantidade_count=("quantidade", "count")
)

df = agregate

selecao = ["codigo", "descricao", "quantidade" , "quantidade_sum", "quantidade_count"]

df = df[selecao].copy()

total = df.groupby(by="codigo")["quantidade_count"].transform("sum")

df["frequencia_relativa"] = round(df["quantidade_count"] / total, 4 )

nome = { "quantidade_sum": "soma", "quantidade_count": "frequencia"}

df.rename(columns=nome, inplace= True)

df.to_csv("tabela_frequencia_geral.csv",index=False)