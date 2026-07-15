CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.populacao_uf_evolucao;

CREATE TABLE marts.populacao_uf_evolucao AS
WITH populacao_uf AS (
    SELECT
        e.sigla            AS estado_sigla,
        e.nome             AS estado_nome,
        e.regiao           AS regiao,
        p.ano              AS ano,
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
    populacao_total,
    LAG(populacao_total) OVER (PARTITION BY estado_sigla ORDER BY ano)
        AS populacao_ano_anterior,
    populacao_total - LAG(populacao_total) OVER (PARTITION BY estado_sigla ORDER BY ano)
        AS variacao_absoluta,
    ROUND(
        100.0 * (populacao_total - LAG(populacao_total) OVER (PARTITION BY estado_sigla ORDER BY ano))
        / LAG(populacao_total) OVER (PARTITION BY estado_sigla ORDER BY ano),
        2
    ) AS variacao_percentual
FROM populacao_uf
ORDER BY estado_sigla, ano;