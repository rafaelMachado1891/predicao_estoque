import pandas as pd
caminho = 'C:/Users/PCP/Documents/repositorios_git/predicao_estoque/movimento_estoque.csv'
# %%
df = pd.read_csv(caminho, delimiter=';')
# %%
df['MOVIMENTO'] = pd.to_numeric(df['MOVIMENTO'], errors='coerce')
# %%
media_consumo = df.groupby(['Codigo'],)['MOVIMENTO'].mean()


