from google.cloud import bigquery

from config.settings import settings

client = bigquery.Client(
    project=settings.GCP_PROJECT_ID
)

def extract_table(table_name: str):
    """
    Extrai uma tabela do projeto Base dos Dados.
    """

    sql = f"""
    SELECT *
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.{table_name}`
    """

    print(f"Extraindo tabela: {table_name}")

    df = client.query(sql).to_dataframe()

    print(f"Total de registros: {len(df):,}")

    return df