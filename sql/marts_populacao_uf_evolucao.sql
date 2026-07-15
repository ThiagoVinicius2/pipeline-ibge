CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.populacao_uf_evolucao;

CREATE TABLE marts.populacao_uf_evolucao AS
WITH populacao_uf AS (
    SELECT
        e.sigla            AS estado_sigla,
        e.nome             AS estado_nome,
        e.regiao           AS regiao,
        p.ano              AS ano,
        CASE
            WHEN p.ano <= 2021 THEN 'pos-censo-2010'
            ELSE 'pos-censo-2022'
        END                AS serie_metodologica,
        SUM(p.populacao)   AS populacao_total
    FROM raw.populacao p
    JOIN raw.municipios m ON m.id = p.municipio_id
    JOIN raw.estados e ON e.id = m.estado_id
    GROUP BY e.sigla, e.nome, e.regiao, p.ano
)
SELECT
    estado_sigla,
    estado_nome,
    regiao,
    ano,
    serie_metodologica,
    populacao_total,
    LAG(populacao_total) OVER (
        PARTITION BY estado_sigla, serie_metodologica ORDER BY ano
    ) AS populacao_ano_anterior,
    populacao_total - LAG(populacao_total) OVER (
        PARTITION BY estado_sigla, serie_metodologica ORDER BY ano
    ) AS variacao_absoluta,
    ROUND(
        100.0 * (populacao_total - LAG(populacao_total) OVER (
            PARTITION BY estado_sigla, serie_metodologica ORDER BY ano
        )) / LAG(populacao_total) OVER (
            PARTITION BY estado_sigla, serie_metodologica ORDER BY ano
        ),
        2
    ) AS variacao_percentual
FROM populacao_uf
ORDER BY estado_sigla, ano;