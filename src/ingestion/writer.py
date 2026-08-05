from pathlib import Path

TABLE_PATHS = {
    "uf": "alfabetizacao",
    "municipio": "municipios",
    "alunos": "alunos",
    "meta_alfabetizacao_brasil": "metas_brasil",
    "meta_alfabetizacao_uf": "metas_uf",
    "meta_alfabetizacao_municipio": "metas_municipios",
    "dicionario": "dicionario",
}

def save_parquet(df, table_name: str):

    folder = TABLE_PATHS.get(table_name, table_name)

    output_dir = Path("data/bronze") / folder
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / f"{table_name}.parquet"

    df.to_parquet(file_path, index=False)

    print(f"Arquivo salvo em: {file_path}")

    return file_path