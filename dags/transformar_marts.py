from __future__ import annotations

from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

SQL_DIR = Path("/opt/airflow/sql")


@task
def executar_sql(nome_arquivo: str) -> None:
    caminho = SQL_DIR / nome_arquivo
    sql = caminho.read_text(encoding="utf-8")

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
    tags=["ibge", "marts"],
)
def transformar_marts():
    executar_sql("marts_municipio_mais_populoso_uf.sql")


transformar_marts()