CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.municipio_mais_populoso_uf;

CREATE TABLE marts.municipio_mais_populoso_uf AS
SELECT DISTINCT ON (e.sigla)
    e.sigla        AS estado_sigla,
    e.nome         AS estado_nome,
    m.nome         AS municipio,
    p.populacao    AS populacao,
    p.ano          AS ano
FROM raw.populacao p
JOIN raw.municipios m ON m.id = p.municipio_id
JOIN raw.estados e ON e.id = m.estado_id
WHERE p.ano = (SELECT MAX(ano) FROM raw.populacao)
ORDER BY e.sigla, p.populacao DESC;