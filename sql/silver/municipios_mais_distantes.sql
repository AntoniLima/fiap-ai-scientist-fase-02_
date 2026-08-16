-- Municipios mais distantes da meta de 2024, na rede Municipal.
--
-- Ranking por distancia_meta (taxa realizada menos meta). Valores
-- negativos indicam quanto falta em pontos percentuais.
--
-- Restrito a situacao_meta = 'comparavel': os demais casos são ausencia
-- estrutural (2023 e ano-base) ou lacuna de cobertura, e comparar sem
-- meta produziria ranking sem sentido.

SELECT
    m.id_municipio,
    m.sigla_uf,
    m.regiao,
    ROUND(m.taxa_alfabetizacao, 1) AS taxa_realizada,
    ROUND(m.meta_alfabetizacao, 1) AS meta,
    ROUND(m.distancia_meta, 1)     AS distancia
FROM meta_vs_resultado m
WHERE m.ano = 2024
  AND m.situacao_meta = 'comparavel'
ORDER BY m.distancia_meta ASC
LIMIT 30
