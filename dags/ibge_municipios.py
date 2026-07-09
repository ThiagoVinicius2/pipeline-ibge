from __future__ import annotations

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

URL_MUNICIPIOS = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"


@task
def extrair_municipios() -> list[dict]:
    resposta = requests.get(URL_MUNICIPIOS, timeout=60)
    resposta.raise_for_status()
    return resposta.json()

@task
def carregar_municipios(municipios: list[dict]) -> None:
    linhas = []
    for m in municipios:
        uf = (
            m.get("microrregiao", {})
            .get("mesorregiao", {})
            .get("UF", {})
        )
        linhas.append((
            m["id"],
            m["nome"],
            uf.get("id"),
            uf.get("sigla"),
        ))

    hook = PostgresHook(postgres_conn_id="warehouse")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.municipios (
            id            INTEGER PRIMARY KEY,
            nome          TEXT,
            estado_id     INTEGER REFERENCES raw.estados(id),
            estado_sigla  TEXT
        );
    """)

    cursor.execute("TRUNCATE TABLE raw.municipios;")

    cursor.executemany(
        "INSERT INTO raw.municipios (id, nome, estado_id, estado_sigla) VALUES (%s, %s, %s, %s);",
        linhas,
    )

    conn.commit()
    cursor.close()
    conn.close()

@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["ibge"],
)
def ibge_municipios():
    municipios = extrair_municipios()
    carregar_municipios(municipios)


ibge_municipios()