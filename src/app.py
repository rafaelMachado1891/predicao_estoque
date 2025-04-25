import pandas as pd

caminho = pd.read_csv("../movimento_estoque.csv",sep=";")
produto_terceiro = pd.read_csv("../produto_terceiros.csv",sep=";")

df_prod= pd.DataFrame(produto_terceiro)
df = pd.DataFrame(caminho)

colunas = ['Codigo', "Descricao", "MES", "ANO", "TOTAL_MOVIMENTO"]
colunas_prod = ["Codigo", "Estoque_Minimo", "Cod_Terc"]

df_prod=df_prod[colunas_prod]
df=df[colunas]

rename_columns={"Codigo": "codigo", "Descricao":"descricao","MES": "mes", "ANO": "ano", "TOTAL_MOVIMENTO": "quantidade"}
rename_columns_prod={"Codigo": "codigo", "Estoque_Minimo": "estoque", "Cod_Terc": "terceiro"}

df.rename(columns=rename_columns,inplace=True)
df_prod.rename(columns=rename_columns_prod, inplace=True)

tipo_dados_prod = {
    "codigo": str,
    "estoque": int,
    "terceiro": int
}

print(df_prod)
df_prod = df_prod.astype(tipo_dados_prod)

df["quantidade"] = df["quantidade"].str.replace(",",".").astype(float)

tipo_dados = {
    "codigo": str,
    "descricao": str,
    "ano": int,
    "mes": int,
    "quantidade": float
}

df = df.astype(tipo_dados)

df=df.groupby(by=["codigo", "descricao","mes", "ano"],as_index=False).agg({"quantidade":["sum"]})

df.columns = ["codigo", "descricao", "mes", "ano", "quantidade_sum"]

print(df)

agregattions = df.groupby(by=["codigo", "descricao"],as_index=False).agg({"quantidade_sum":["sum", "min", "max", "mean", "std", "var", "count"]})

print(agregattions)

agregattions.to_csv("calculo.csv",index=False)