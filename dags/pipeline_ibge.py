from __future__ import annotations

from pathlib import Path

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

URL_ESTADOS = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
URL_MUNICIPIOS = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

URL_POPULACAO = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/6579"
    "/periodos/{ano}/variaveis/9324?localidades=N6[all]"
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
    linhas = [(e["id"], e["sigla"], e["nome"], e["regiao"]["nome"]) for e in estados]
    cursor.executemany(
        """
        INSERT INTO raw.estados (id, sigla, nome, regiao)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            sigla  = EXCLUDED.sigla,
            nome   = EXCLUDED.nome,
            regiao = EXCLUDED.regiao;
        """,
        linhas,
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
    cursor.executemany(
        """
        INSERT INTO raw.municipios (id, nome, estado_id, estado_sigla)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            nome         = EXCLUDED.nome,
            estado_id    = EXCLUDED.estado_id,
            estado_sigla = EXCLUDED.estado_sigla;
        """,
        linhas,
    )
    conn.commit()
    cursor.close()
    conn.close()


@task
def extrair_populacao(data_interval_start=None) -> dict:
    ano = str(data_interval_start.year)
    url = URL_POPULACAO.format(ano=ano)

    resposta = requests.get(url, timeout=60)
    resposta.raise_for_status()
    dados = resposta.json()

    # A API devolve [] com status 200 para anos sem estimativa (ex.: censos).
    if not dados:
        return {"ano": ano, "series": []}

    return {"ano": ano, "series": dados[0]["resultados"][0]["series"]}


@task
def carregar_populacao(payload: dict) -> None:
    ano = payload["ano"]
    series = payload["series"]

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

    if not series:
        print(f"Sem estimativa de populacao para {ano}. Nada a carregar.")
        conn.commit()
        cursor.close()
        conn.close()
        return

    linhas = []
    for item in series:
        municipio_id = int(item["localidade"]["id"])
        populacao_str = item["serie"].get(ano)
        if populacao_str is None or not populacao_str.isdigit():
            continue
        linhas.append((municipio_id, int(ano), int(populacao_str)))

    cursor.execute("DELETE FROM raw.populacao WHERE ano = %s;", (int(ano),))
    cursor.executemany(
        "INSERT INTO raw.populacao (municipio_id, ano, populacao) VALUES (%s, %s, %s);",
        linhas,
    )
    print(f"Carregadas {len(linhas)} linhas de populacao para {ano}.")

    conn.commit()
    cursor.close()
    conn.close()


@task
def transformar_marts() -> None:
    arquivos = [
        "marts_municipio_mais_populoso_uf.sql",
        "marts_populacao_uf_evolucao.sql",
    ]

    hook = PostgresHook(postgres_conn_id="warehouse")
    conn = hook.get_conn()
    cursor = conn.cursor()

    for nome in arquivos:
        sql = (SQL_DIR / nome).read_text(encoding="utf-8")
        cursor.execute(sql)
        print(f"Executado: {nome}")

    conn.commit()
    cursor.close()
    conn.close()


@dag(
    schedule="@yearly",
    start_date=pendulum.datetime(2015, 1, 1, tz="America/Sao_Paulo"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(seconds=30),
    },
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