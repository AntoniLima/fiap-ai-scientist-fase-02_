from ingestion.extract import extract_table
from ingestion.writer import save_parquet


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

        save_parquet(df, table)

    print("=" * 60)
    print("PROCESSO FINALIZADO")
    print("=" * 60)


if __name__ == "__main__":
    main()