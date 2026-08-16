from config.settings import settings
from google.cloud import bigquery

client = bigquery.Client(
    project=settings.GCP_PROJECT_ID
)

sql = """
SELECT *
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.uf`
LIMIT 1000
"""

df = client.query(sql).to_dataframe()

print(df.head())