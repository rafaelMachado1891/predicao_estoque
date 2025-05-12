import pandas as pd

caminho = pd.read_csv("../estoque_correio.csv", sep=";")

print(caminho)
df = pd.DataFrame(caminho)
print(df.head())