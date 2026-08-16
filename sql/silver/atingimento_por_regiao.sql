-- Atingimento da meta de 2024 por região, na rede Municipal.
--
-- Mostra a dispersão regional do indicador: onde a meta foi alcançada
-- pela maioria e onde a distancia média e maior.

SELECT
    regiao,
    COUNT(*)                                                   AS municipios,
    SUM(CASE WHEN atingiu_meta THEN 1 ELSE 0 END)              AS atingiram,
    ROUND(100.0 * SUM(CASE WHEN atingiu_meta THEN 1 ELSE 0 END)
          / COUNT(*), 1)                                       AS percentual,
    ROUND(AVG(taxa_alfabetizacao), 1)                          AS taxa_media,
    ROUND(AVG(distancia_meta), 1)                              AS distancia_media
FROM meta_vs_resultado
WHERE ano = 2024
  AND situacao_meta = 'comparavel'
GROUP BY regiao
ORDER BY percentual DESC
