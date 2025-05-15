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

agregate = df.groupby(by=["codigo", "descricao"],as_index=False).agg({"quantidade":["sum","min","max","var","mean","std","count"]})

agregate.columns = ['_'.join(col).strip('_') for col in agregate.columns.values]

df = agregate

df["calculo_estoque"] = round(df["quantidade_mean"] + (2 * df["quantidade_std"]),0)

df["amplitude"] = df["quantidade_min"] - df["quantidade_max"]

selecao = ["codigo", "descricao", "quantidade_sum", "quantidade_mean", "calculo_estoque", "quantidade_count","quantidade_min","quantidade_max" ,"quantidade_var", "amplitude","quantidade_std"]

df=df[selecao]

df.to_csv("correios.csv",index=False)

