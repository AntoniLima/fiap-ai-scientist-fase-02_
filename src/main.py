from ingestion.extract import extract_table
from ingestion.writer import save_parquet
from cloud.s3 import upload_file


TABLES = [
    "uf",
    "municipio",
    "alunos",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "dicionario",
]

def main():

    print("=" * 60)
    print("INICIANDO EXTRAÇÃO BASE DOS DADOS")
    print("=" * 60)

    for table in TABLES:

        print("-" * 60)

        df = extract_table(table)

        file_path = save_parquet(df, table)

        upload_file(file_path)

    print("=" * 60)
    print("PROCESSO FINALIZADO")
    print("=" * 60)


if __name__ == "__main__":
    main()