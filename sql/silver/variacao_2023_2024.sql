-- Variação da taxa de alfabetização entre 2023 e 2024, por UF.
--
-- São dois pontos no tempo: isso sustenta comparação entre anos, não
-- afirmação de tendência. A cobertura da fonte vai de 2023 a 2024.

WITH por_ano AS (
    SELECT
        sigla_uf,
        ano,
        AVG(taxa_alfabetizacao) AS taxa
    FROM fato_indicador_municipio
    WHERE rede_codigo = '3'
    GROUP BY sigla_uf, ano
)
SELECT
    a.sigla_uf,
    ROUND(a.taxa, 1)            AS taxa_2023,
    ROUND(b.taxa, 1)            AS taxa_2024,
    ROUND(b.taxa - a.taxa, 1)   AS variacao
FROM por_ano a
JOIN por_ano b
  ON a.sigla_uf = b.sigla_uf
 AND a.ano = 2023
 AND b.ano = 2024
ORDER BY variacao DESC
