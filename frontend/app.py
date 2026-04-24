from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus
import os


ROOT_DIR = Path(__file__).resolve().parents[1]
MARTS_DIR = ROOT_DIR / "db_correios" / "models" / "marts"
APP_ENV_FILE = ROOT_DIR / "app" / ".env"
LOCAL_ENV_FILE = Path(__file__).resolve().parent / ".env"
DEFAULT_SCHEMA = "marts"


def load_environment() -> None:
    if APP_ENV_FILE.exists():
        load_dotenv(APP_ENV_FILE)
    if LOCAL_ENV_FILE.exists():
        load_dotenv(LOCAL_ENV_FILE, override=True)


def prettify_name(name: str) -> str:
    parts = re.split(r"[._]+", name)
    return " ".join(part.capitalize() for part in parts if part)


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def get_db_config() -> dict[str, str]:
    return {
        "host": os.getenv("HOST_POSTGRES", "localhost"),
        "port": os.getenv("PORT_POSTGRES", "5433"),
        "database": os.getenv("DB_POSTGRES", "correios_db"),
        "user": os.getenv("USER_POSTGRES", "admin"),
        "password": os.getenv("PASSWORD_POSTGRES", ""),
        "schema": os.getenv("DBT_MARTS_SCHEMA", DEFAULT_SCHEMA),
    }


def build_connection_url() -> str:
    config = get_db_config()
    password = quote_plus(config["password"])
    return (
        f"postgresql+psycopg2://{config['user']}:{password}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    return create_engine(build_connection_url(), pool_pre_ping=True)


def discover_reports() -> list[dict[str, str]]:
    reports: list[dict[str, str]] = []
    if not MARTS_DIR.exists():
        return reports

    for sql_file in sorted(MARTS_DIR.glob("*.sql")):
        report_id = sql_file.stem
        reports.append(
            {
                "id": report_id,
                "title": prettify_name(report_id),
                "path": str(sql_file),
                "sql": sql_file.read_text(encoding="utf-8"),
            }
        )
    return reports


@st.cache_data(show_spinner=False, ttl=60)
def list_available_tables() -> set[str]:
    config = get_db_config()
    inspector = inspect(get_engine())
    return set(inspector.get_table_names(schema=config["schema"]))


@st.cache_data(show_spinner=False, ttl=60)
def load_report_data(report_id: str, limit: int) -> pd.DataFrame:
    config = get_db_config()
    relation = (
        f"{quote_identifier(config['schema'])}.{quote_identifier(report_id)}"
    )
    query = text(f"SELECT * FROM {relation} LIMIT :limit")
    return pd.read_sql_query(query, get_engine(), params={"limit": limit})


@st.cache_data(show_spinner=False, ttl=60)
def count_report_rows(report_id: str) -> int:
    config = get_db_config()
    relation = (
        f"{quote_identifier(config['schema'])}.{quote_identifier(report_id)}"
    )
    query = text(f"SELECT COUNT(*) AS total_rows FROM {relation}")
    with get_engine().connect() as connection:
        return int(connection.execute(query).scalar_one())


def apply_filters(dataframe: pd.DataFrame) -> pd.DataFrame:
    filtered = dataframe.copy()
    searchable_columns = list(filtered.columns)

    search_text = st.text_input(
        "Buscar texto em qualquer coluna",
        placeholder="Ex.: EC123, semana 12, estoque...",
    ).strip()
    if search_text:
        mask = pd.Series(False, index=filtered.index)
        lowered = search_text.lower()
        for column in searchable_columns:
            mask |= filtered[column].astype(str).str.lower().str.contains(
                lowered,
                na=False,
            )
        filtered = filtered.loc[mask]

    date_columns = filtered.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns
    for column in date_columns:
        col_min = filtered[column].min()
        col_max = filtered[column].max()
        if pd.isna(col_min) or pd.isna(col_max):
            continue
        selected_range = st.date_input(
            f"Intervalo para {column}",
            value=(col_min.date(), col_max.date()),
            key=f"date_{column}",
        )
        if isinstance(selected_range, tuple) and len(selected_range) == 2:
            start_date, end_date = selected_range
            filtered = filtered[
                filtered[column].dt.date.between(start_date, end_date)
            ]

    return filtered


def render_sidebar(reports: list[dict[str, str]], available_tables: set[str]) -> dict[str, str]:
    st.sidebar.title("Relatorios DBT")
    st.sidebar.caption("Camada `marts` materializada no Postgres")

    search = st.sidebar.text_input(
        "Filtrar relatorios",
        placeholder="Digite parte do nome...",
    ).strip().lower()

    visible_reports = [
        report
        for report in reports
        if not search
        or search in report["id"].lower()
        or search in report["title"].lower()
    ]

    if not visible_reports:
        st.sidebar.warning("Nenhum relatorio encontrado com esse filtro.")
        return reports[0]

    selected_id = st.sidebar.radio(
        "Escolha o relatorio",
        options=[report["id"] for report in visible_reports],
        format_func=lambda report_id: next(
            report["title"] for report in visible_reports if report["id"] == report_id
        ),
    )

    selected_report = next(
        report for report in reports if report["id"] == selected_id
    )

    if selected_report["id"] in available_tables:
        st.sidebar.success("Tabela encontrada no schema marts.")
    else:
        st.sidebar.error("Tabela ainda nao encontrada no schema marts.")

    return selected_report


def render_connection_status() -> None:
    config = get_db_config()
    st.caption(
        "Conexao atual: "
        f"`{config['host']}:{config['port']}` | "
        f"banco `{config['database']}` | schema `{config['schema']}`"
    )


def main() -> None:
    load_environment()
    st.set_page_config(
        page_title="Relatorios DBT Correios",
        page_icon=":bar_chart:",
        layout="wide",
    )

    st.title("Visualizador de Relatorios DBT")
    st.write(
        "Selecione qualquer modelo da camada `marts` para consultar os dados "
        "materializados no Postgres."
    )
    render_connection_status()

    reports = discover_reports()
    if not reports:
        st.error(
            "Nenhum arquivo SQL foi encontrado em `db_correios/models/marts`."
        )
        st.stop()

    try:
        available_tables = list_available_tables()
    except Exception as exc:
        st.error("Nao foi possivel listar as tabelas do schema `marts`.")
        st.exception(exc)
        st.stop()

    selected_report = render_sidebar(reports, available_tables)

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        limit = st.number_input(
            "Limite de linhas",
            min_value=10,
            max_value=100000,
            value=1000,
            step=10,
        )
    with col2:
        if st.button("Atualizar consulta"):
            load_report_data.clear()
            count_report_rows.clear()
            list_available_tables.clear()
            st.rerun()
    with col3:
        st.info(f"Relatorio selecionado: `{selected_report['id']}`")

    if selected_report["id"] not in available_tables:
        st.warning(
            "O arquivo SQL existe, mas a tabela correspondente ainda nao foi encontrada "
            "no banco. Rode o `dbt run` para materializar esse relatorio."
        )
        with st.expander("Ver SQL do relatorio"):
            st.code(selected_report["sql"], language="sql")
        st.stop()

    try:
        total_rows = count_report_rows(selected_report["id"])
        dataframe = load_report_data(selected_report["id"], int(limit))
    except Exception as exc:
        st.error("Nao foi possivel consultar o relatorio selecionado.")
        st.exception(exc)
        with st.expander("Ver SQL do relatorio"):
            st.code(selected_report["sql"], language="sql")
        st.stop()

    for column in dataframe.columns:
        if dataframe[column].dtype == object:
            converted = pd.to_datetime(dataframe[column], errors="coerce")
            if converted.notna().all():
                dataframe[column] = converted

    filtered_dataframe = apply_filters(dataframe)

    metric_1, metric_2, metric_3 = st.columns(3)
    metric_1.metric("Linhas no banco", f"{total_rows:,}".replace(",", "."))
    metric_2.metric("Linhas exibidas", f"{len(filtered_dataframe):,}".replace(",", "."))
    metric_3.metric("Colunas", len(filtered_dataframe.columns))

    st.dataframe(filtered_dataframe, use_container_width=True, hide_index=True)

    csv_data = filtered_dataframe.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Baixar CSV",
        data=csv_data,
        file_name=f"{selected_report['id']}.csv",
        mime="text/csv",
    )

    with st.expander("Ver SQL do relatorio"):
        st.code(selected_report["sql"], language="sql")


if __name__ == "__main__":
    main()
