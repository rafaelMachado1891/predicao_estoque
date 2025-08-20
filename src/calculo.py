import pandas as pd

# === 1. Leitura e preparação inicial ===
caminho = pd.read_csv("../estoque_correios.csv", sep=",")

colunas = ["Numero", "Data_EM", "Codigo", "Descricao", "Quantidade", "Pedido", "MARCA", "GRUPO", "NUMERO_SEMANA", "ANO"]
df = caminho[colunas]

rename_columns = {
    "Numero": "numero", "Data_EM": "data", "Codigo": "codigo", "Descricao": "descricao",
    "Quantidade": "quantidade", "Pedido": "pedido", "MARCA": "marca", "GRUPO": "grupo", "NUMERO_SEMANA": "numero_semana", "ANO": "ano"
}
df.rename(columns=rename_columns, inplace=True)

df = df.astype({
    "numero": int,
    "data": object,
    "codigo": str,
    "descricao": str,
    "quantidade": int,
    "pedido": int,
    "marca": str,
    "grupo": str,
    "numero_semana": int,
    "ano": int
})

# === 2. Conversão da data e filtro ===
df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")
data_consulta = input("A partir de qual data você deseja consultar? (ano/mes/dia): ")
data_consulta = pd.to_datetime(data_consulta, format="%Y/%m/%d", errors="coerce")
df = df.loc[df["data"] >= data_consulta]

df["descricao"] = df["descricao"].str.rstrip()

df_estatisticas = df.groupby(["codigo", "descricao", "ano", "numero_semana"], as_index=False).agg(
quantidade_sum=("quantidade", "sum"))

print(df_estatisticas)

# === 3. Estatísticas por produto ===
df_estatisticas = df_estatisticas.groupby(["codigo", "descricao"], as_index=False).agg(
    quantidade_sum=("quantidade_sum", "sum"),
    quantidade_min=("quantidade_sum", "min"),
    quantidade_max=("quantidade_sum", "max"),
    quantidade_var=("quantidade_sum", "var"),
    quantidade_mean=("quantidade_sum", "mean"),
    quantidade_std=("quantidade_sum", "std"),
    quantidade_count=("quantidade_sum", "count"),
    quantidade_median=("quantidade_sum", "median"),
    quantidade_q1=("quantidade_sum", lambda x: x.quantile(0.25)),
    quantidade_q3=("quantidade_sum", lambda x: x.quantile(0.75))
)

df_estatisticas["calculo_estoque"] = round(df_estatisticas["quantidade_mean"] + (1.5 * df_estatisticas["quantidade_std"]), 0)
df_estatisticas["amplitude"] = df_estatisticas["quantidade_max"] - df_estatisticas["quantidade_min"]

# Seleção e renomeação final das colunas
df_estatisticas = df_estatisticas[[
    "codigo", "descricao", "quantidade_sum", "quantidade_mean", "calculo_estoque",
    "quantidade_min", "quantidade_max", "quantidade_var", "amplitude",
    "quantidade_std", "quantidade_count", "quantidade_median", "quantidade_q1", "quantidade_q3"
]]

df_estatisticas.rename(columns={
    "quantidade_sum": "soma",
    "quantidade_mean": "media",
    "calculo_estoque": "calculo",
    "quantidade_min": "minimo",
    "quantidade_max": "maximo",
    "quantidade_var": "variancia",
    "quantidade_std": "desvio_padrao",
    "quantidade_count": "frequencia"
}, inplace=True)

# Frequência relativa geral
total_geral = df_estatisticas["frequencia"].sum()
df_estatisticas["frequencia_relativa"] = round(df_estatisticas["frequencia"] / total_geral, 4)

# === 4. Frequência relativa por quantidade ===
df_freq = df.groupby(["codigo", "quantidade"], as_index=False).agg(
    quantidade_count=("quantidade", "count")
)

df_freq["total_produto"] = df_freq.groupby("codigo")["quantidade_count"].transform("sum")
df_freq["frequencia_relativa"] = round(df_freq["quantidade_count"] / df_freq["total_produto"], 4)

# Pivotando: cada coluna será uma quantidade
df_pivot = df_freq.pivot(index="codigo", columns="quantidade", values="frequencia_relativa").fillna(0)
df_pivot.columns = [f"qtd_{int(col)}_emb" for col in df_pivot.columns]
df_pivot.reset_index(inplace=True)

# === 5. Merge final ===
df_final = df_estatisticas.merge(df_pivot, on="codigo", how="left")

# === 6. Exportar resultado ===
df_final.to_csv("relatorio_produtos_completo2.csv", index=False)
print("✅ Relatório final gerado: 'relatorio_produtos_completo.csv'")