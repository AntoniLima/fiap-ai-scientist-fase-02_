-- Registros em quarentena, por motivo.
--
-- Quarentena não é descarte: são linhas contabilizadas, com o motivo
-- registrado. Um município aqui é um município que precisa de meta
-- publicada, não um município a ignorar.

SELECT
    motivo_quarentena,
    COUNT(*)                        AS linhas,
    COUNT(DISTINCT id_municipio)    AS municipios,
    COUNT(DISTINCT sigla_uf)        AS ufs
FROM quarentena
GROUP BY motivo_quarentena
ORDER BY linhas DESC
