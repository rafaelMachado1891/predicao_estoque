# Frontend Streamlit

Aplicacao Streamlit para visualizar os relatorios da camada `db_correios/models/marts`.

## O que ela faz

- Lista automaticamente os arquivos `.sql` da pasta `marts`
- Consulta a tabela materializada correspondente no schema `marts`
- Exibe os dados em grade
- Permite busca textual, filtro por data e exportacao CSV
- Mostra o SQL do relatorio selecionado

## Como executar

1. Ative seu ambiente virtual.
2. Instale as dependencias:

```powershell
pip install -r frontend/requirements.txt
```

3. Garanta que o Postgres do `app/docker-compose.yml` esteja no ar e que os modelos dbt da camada `marts` tenham sido executados.
4. Inicie a interface:

```powershell
streamlit run frontend/app.py
```

## Configuracao

Por padrao a aplicacao reaproveita as variaveis do arquivo `app/.env`.

Se quiser sobrescrever algo so para o frontend, crie `frontend/.env` com valores como:

```env
HOST_POSTGRES=localhost
PORT_POSTGRES=5433
DB_POSTGRES=correios_db
USER_POSTGRES=admin
PASSWORD_POSTGRES=1891
DBT_MARTS_SCHEMA=marts
```
