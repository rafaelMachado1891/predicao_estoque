
import pandas as pd

# Carregar o arquivo CSV
caminho = pd.read_csv("../movimento_estoque.csv", sep=";")

# Criar o DataFrame
df = pd.DataFrame(caminho)

# Selecionar as colunas desejadas
colunas = ['Codigo', "Descricao", "MES", "ANO", "TOTAL_MOVIMENTO"]
df = df[colunas]

# Renomear as colunas
rename_columns = {"Codigo": "codigo", "Descricao": "descricao", "MES": "mes", "ANO": "ano", "TOTAL_MOVIMENTO": "quantidade"}
df.rename(columns=rename_columns, inplace=True)

# Remover vírgulas e converter quantidade para float
df["quantidade"] = df["quantidade"].str.replace(",", ".").astype(float)

# Filtrar pelo código desejado
filtro = df["codigo"] == 577
df = df[filtro]

# Agrupar por código, descrição, mês e ano e calcular a soma da quantidade
df = df.groupby(by=["codigo", "descricao", "mes", "ano"], as_index=False).agg({"quantidade": ["sum"]})

# Ajustar o nome da coluna após a primeira agregação
df.columns = ['codigo', 'descricao', 'mes', 'ano', 'quantidade_sum']

# Agrupar por código e descrição e calcular as estatísticas desejadas
df = df.groupby(by=["codigo", "descricao"], as_index=False).agg({"quantidade_sum": ["sum", "min", "max", "mean", "std", "var", "count"]
})

# Exibir o DataFrame final
print(df)
