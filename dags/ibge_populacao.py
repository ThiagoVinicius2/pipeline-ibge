from __future__ import annotations

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

ANO = "2021"
URL_POPULACAO = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/6579"
    f"/periodos/{ANO}/variaveis/9324?localidades=N6[all]"
)


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
            municipio_id  INTEGER REFERENCES raw.municipios(id),
            ano           INTEGER,
            populacao     INTEGER,
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

@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["ibge"],
)
def ibge_populacao():
    series = extrair_populacao()
    carregar_populacao(series)


ibge_populacao()