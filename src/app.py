import pandas as pd

caminho = pd.read_csv("../movimento_estoque.csv",sep=";")
produto_terceiro = pd.read_csv("../prod_terc.csv",sep=";")

df_prod= pd.DataFrame(produto_terceiro)
df = pd.DataFrame(caminho)

colunas = ['Codigo', "Descricao", "MES", "ANO", "TOTAL_MOVIMENTO"]
colunas_prod = ["Codigo", "Estoque_minimo", "Cod_Terc", "Razao" , "Lead_time"]

df_prod=df_prod[colunas_prod]
df=df[colunas]

rename_columns={"Codigo": "codigo", "Descricao":"descricao","MES": "mes", "ANO": "ano", "TOTAL_MOVIMENTO": "quantidade"}
rename_columns_prod={"Codigo": "codigo", "Estoque_minimo": "estoque", "Cod_Terc": "terceiro", "Razao": "razao",  "Lead_time": "lead_time"}

df.rename(columns=rename_columns,inplace=True)
df_prod.rename(columns=rename_columns_prod, inplace=True)

tipo_dados_prod = {
    "codigo": str,
    "estoque": int,
    "terceiro": int,
    "razao": str, 
    "lead_time": int
}

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

agregattions = df.groupby(by=["codigo", "descricao"],as_index=False).agg({"quantidade_sum":["sum", "min", "max", "mean", "std", "var", "count"]})

agregattions.columns = ["codigo", "descricao", "soma_qtde", "min_qtde", "max_qtde", 
                        "media_qtde", "desvio_padrao", "variancia", "contagem"]


agregattions= agregattions.merge(df_prod,left_on=["codigo"], right_on=["codigo"],how="inner")

fator_seguranca = float(input("Digite o fator de seguranca desejado [1, 1.5 , 2]: "))

agregattions["calculo_estoque"] = round(agregattions["media_qtde"] / 22 * agregattions["lead_time"] + ( agregattions["desvio_padrao"] * fator_seguranca ),0)

agregattions["media_qtde"] = round(agregattions["media_qtde"],0)
agregattions["desvio_padrao"] = round(agregattions["desvio_padrao"], 0)


resultado = ["codigo", "descricao", "estoque", "soma_qtde", "media_qtde", "lead_time", "desvio_padrao", "calculo_estoque", "razao"]

agregattions= agregattions[resultado]

agregattions["situacao"] = agregattions.apply(
    lambda row: "menor" if row["calculo_estoque"] > row["estoque"] else "maior",
    axis=1
)

print(agregattions)

# agregattions.to_csv("calculo.csv",index=False)