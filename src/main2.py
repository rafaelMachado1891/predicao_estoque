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

agregate = df.groupby(by=["codigo", "descricao"],as_index=False).agg({"quantidade":["sum","min","max","var","mean","std","count",
                                                                                     "median", lambda x: x.quantile(0.25), 
                                                                                               lambda x: x.quantile(0.75)]})

agregate.columns = ['_'.join(col).strip('_') for col in agregate.columns.values]

df = agregate

df["calculo_estoque"] = round(df["quantidade_mean"] + (3 * df["quantidade_std"]),0)

df["amplitude"] = df["quantidade_max"] - df["quantidade_min"]

selecao = ["codigo", "descricao", "quantidade_sum", "quantidade_mean", "calculo_estoque","quantidade_min","quantidade_max" ,"quantidade_var", "amplitude",
           "quantidade_std", "quantidade_count", "quantidade_median", "quantidade_quantile(0.25)", "quantidade_quantile(0.75)"]

df = df[selecao].copy()

total = df["quantidade_count"].sum()

df["frequencia_relativa"] = round(df["quantidade_count"] / total, 4 )

nome = { "quantidade_sum": "soma", "quantidade_mean": "media", "calculo_estoque": "calculo", "quantidade_min": "minimo", "quantidade_max": "maximo", "quantidade_var": "variancia", 
         "quantidade_std": "desvio_padrao", "quantidade_count": "frequencia"}

df.rename(columns=nome, inplace= True)

df.to_csv("correios.csv",index=False)

