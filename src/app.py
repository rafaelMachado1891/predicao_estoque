import pandas as pd

caminho = pd.read_csv("../movimento_estoque.csv",sep=";")

df = pd.DataFrame(caminho)

filtro = df["Codigo"] == 577

df = df[filtro]

# Certifica-se de que a coluna 'Total_Movimento' está numérica
df["Total_Movimento"] = pd.to_numeric(df["TOTAL_MOVIMENTO"], errors="coerce")

print(df["TOTAL_MOVIMENTO"].describe())
