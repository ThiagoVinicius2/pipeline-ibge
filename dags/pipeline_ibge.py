from __future__ import annotations

from pathlib import Path

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

URL_ESTADOS = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
URL_MUNICIPIOS = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

ANO = "2021"
URL_POPULACAO = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/6579"
    f"/periodos/{ANO}/variaveis/9324?localidades=N6[all]"
)

SQL_DIR = Path("/opt/airflow/sql")

@task
def extrair_estados() -> list[dict]:
    resposta = requests.get(URL_ESTADOS, timeout=30)
    resposta.raise_for_status()
    return resposta.json()


@task
def carregar_estados(estados: list[dict]) -> None:
    hook = PostgresHook(postgres_conn_id="warehouse")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.estados (
            id INTEGER PRIMARY KEY, sigla TEXT, nome TEXT, regiao TEXT
        );
    """)
    cursor.execute("TRUNCATE TABLE raw.estados CASCADE;")
    for e in estados:
        cursor.execute(
            "INSERT INTO raw.estados (id, sigla, nome, regiao) VALUES (%s, %s, %s, %s);",
            (e["id"], e["sigla"], e["nome"], e["regiao"]["nome"]),
        )
    conn.commit()
    cursor.close()
    conn.close()

@task
def extrair_municipios() -> list[dict]:
    resposta = requests.get(URL_MUNICIPIOS, timeout=60)
    resposta.raise_for_status()
    return resposta.json()


@task
def carregar_municipios(municipios: list[dict]) -> None:
    linhas = []
    for m in municipios:
        micro = m.get("microrregiao") or {}
        meso = micro.get("mesorregiao") or {}
        uf = meso.get("UF") or {}
        linhas.append((m["id"], m["nome"], uf.get("id"), uf.get("sigla")))

    hook = PostgresHook(postgres_conn_id="warehouse")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.municipios (
            id INTEGER PRIMARY KEY, nome TEXT,
            estado_id INTEGER REFERENCES raw.estados(id), estado_sigla TEXT
        );
    """)
    cursor.execute("TRUNCATE TABLE raw.municipios CASCADE;")
    cursor.executemany(
        "INSERT INTO raw.municipios (id, nome, estado_id, estado_sigla) VALUES (%s, %s, %s, %s);",
        linhas,
    )
    conn.commit()
    cursor.close()
    conn.close()

@task
def extrair_populacao() -> list[dict]:
    resposta = requests.get(URL_POPULACAO, timeout=60)
    resposta.raise_for_status()
    dados = resposta.json()
    return dados[0]["resultados"][0]["series"]


@task
def carregar_populacao(series: list[dict]) -> None:
    linhas = []
    for item in series:
        municipio_id = int(item["localidade"]["id"])
        populacao_str = item["serie"].get(ANO)
        if populacao_str is None or not populacao_str.isdigit():
            continue
        linhas.append((municipio_id, int(ANO), int(populacao_str)))

    hook = PostgresHook(postgres_conn_id="warehouse")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.populacao (
            municipio_id INTEGER REFERENCES raw.municipios(id),
            ano INTEGER, populacao INTEGER,
            PRIMARY KEY (municipio_id, ano)
        );
    """)
    cursor.execute("DELETE FROM raw.populacao WHERE ano = %s;", (int(ANO),))
    cursor.executemany(
        "INSERT INTO raw.populacao (municipio_id, ano, populacao) VALUES (%s, %s, %s);",
        linhas,
    )
    conn.commit()
    cursor.close()
    conn.close()


@task
def transformar_marts() -> None:
    sql = (SQL_DIR / "marts_municipio_mais_populoso_uf.sql").read_text(encoding="utf-8")
    hook = PostgresHook(postgres_conn_id="warehouse")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["ibge", "pipeline"],
)
def pipeline_ibge():
    estados = extrair_estados()
    estados_carregados = carregar_estados(estados)

    municipios = extrair_municipios()
    municipios_carregados = carregar_municipios(municipios)

    populacao = extrair_populacao()
    populacao_carregada = carregar_populacao(populacao)

    marts = transformar_marts()

    estados_carregados >> municipios_carregados >> populacao_carregada >> marts


pipeline_ibge()