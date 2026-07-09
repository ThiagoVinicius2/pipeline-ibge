from __future__ import annotations

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

URL_ESTADOS = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"

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
            id      INTEGER PRIMARY KEY,
            sigla   TEXT,
            nome    TEXT,
            regiao  TEXT
        );
    """)

    cursor.execute("TRUNCATE TABLE raw.estados;")

    for estado in estados:
        cursor.execute(
            "INSERT INTO raw.estados (id, sigla, nome, regiao) VALUES (%s, %s, %s, %s);",
            (estado["id"], estado["sigla"], estado["nome"], estado["regiao"]["nome"]),
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
def ibge_estados():
    estados = extrair_estados()
    carregar_estados(estados)

ibge_estados()