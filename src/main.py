# %%
# %% Importação de bibliotecas
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error


# %% Carregar dados
df = pd.read_csv(caminho, delimiter=';')

# %% Verificar a estrutura dos dados
print(df.info())
print(df.describe())

# %% Substituir vírgulas por pontos nas colunas numéricas e converter para float
for col in ['TOTAL_MOVIMENTO', 'TOTAL_COMPRAS_PRODUTO', 
            'RELACAO_COMPRAS_GERAL_PRODUTO', 'PERCENTUAL_COMPRAS_FAT']:
    df[col] = df[col].str.replace(',', '.').astype(float)

# %% Garantir que 'TOTAL_MOVIMENTO' seja numérico
df['TOTAL_MOVIMENTO'] = pd.to_numeric(df['TOTAL_MOVIMENTO'], errors='coerce')

# %% Agregar o consumo mensal por produto
consumo_agregate = df.groupby(['Codigo', 'ANO', 'MES'], as_index=False)['TOTAL_MOVIMENTO'].sum()
consumo_agregate.rename(columns={'TOTAL_MOVIMENTO': 'TOTAL_MOVIMENTO_AGREGADO'}, inplace=True)

# %% Combinar os dados agregados com o original
df = df.merge(consumo_agregate, on=['Codigo', 'ANO', 'MES'], how='left')

# %% Criar lag feature (movimentação no mês anterior)
df['total_movimento_lag1'] = df.groupby('Codigo')['TOTAL_MOVIMENTO_AGREGADO'].shift(1)

# %% Remover valores nulos gerados pela lag e garantir que não haja NaNs
df = df.dropna()

# %% Seleção de features e target
features = [
    'MES', 'ANO', 'TOTAL_COMPRAS_PRODUTO',
    'RELACAO_COMPRAS_GERAL_PRODUTO', 'PERCENTUAL_COMPRAS_FAT',
    'total_movimento_lag1'
]
target = 'TOTAL_MOVIMENTO_AGREGADO'

X = df[features]
y = df[target]

# %% Dividir os dados em treino e teste
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# %% Treinar o modelo (Random Forest como exemplo)
model = RandomForestRegressor(random_state=42)
model.fit(X_train, y_train)

# %% Avaliar o modelo
y_pred = model.predict(X_test)
mae = mean_absolute_error(y_test, y_pred)
print(f"Erro Médio Absoluto: {mae}")

# %% Visualizar previsões vs valores reais
plt.figure(figsize=(10, 6))
plt.plot(y_test.to_numpy(), label="Valores Reais", color="blue")
plt.plot(y_pred, label="Previsões", color="orange")
plt.legend()
plt.title("Previsão de Movimentação Mensal")
plt.show()