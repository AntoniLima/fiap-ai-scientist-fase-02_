-- Distribuição dos alunos em relação ao ponto de corte de 743 pontos.
--
-- A faixa proximo_abaixo reune os alunos a menos de 50 pontos do corte:
-- e o grupo com maior retorno marginal de intervenção pedagógica, e o
-- que a média da taxa de alfabetização esconde.
--
-- Considera apenas alunos válidos — presentes e com prova preenchida.
-- A coluna alfabetizado vale false para quem não fez a prova, então
-- agregar sem esse filtro produz número diferente do oficial.

SELECT
    faixa_proximidade,
    COUNT(*)                                        AS alunos,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentual,
    ROUND(AVG(proficiencia), 1)                     AS proficiencia_media
FROM fato_aluno
WHERE ano = 2024
  AND aluno_valido
GROUP BY faixa_proximidade
ORDER BY proficiencia_media
