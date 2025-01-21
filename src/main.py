# %%
import pandas as pd
caminho = 'C:/Users/PCP/Documents/repositorios_git/predicao_estoque/movimento_estoque.csv'
# %%
df = pd.read_csv(caminho, delimiter=';')
# %%
df['TOTAL_MOVIMENTO'] = pd.to_numeric(df['TOTAL_MOVIMENTO'], errors='coerce')
# %%
consumo_agregate = df.groupby(['Codigo', 'ANO', 'MES'])['TOTAL_MOVIMENTO'].sum().reset_index()

# %%
consumo_agregate

# %%
df = df.merge(consumo_agregate, on=['Codigo', 'ANO', 'MES'])


# %%
df
# %%
