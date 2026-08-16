"""
Camada Silver — Glue Job (PySpark).

Lê a Bronze pelo Glue Catalog, aplica limpeza, padronização e integração,
e grava a Silver no S3 com schema explícito.

Parâmetros do Job:
    --JOB_NAME          nome do job (injetado pelo Glue)
    --BUCKET_ORIGEM     bucket de leitura da Bronze
    --BUCKET_DESTINO    bucket de escrita da Silver
    --DATABASE_BRONZE   database de origem no Catalog
    --ENV               dev | prod
    --FONTE             catalog | s3
"""

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ===========================================================================
# CONTRATO — decisões de transformação, verificadas contra a Bronze completa
# ===========================================================================

PONTO_CORTE_ALFABETIZACAO = 743

# A série de metas começa em 2024: 2023 é ano-base, sem meta por definição.
PRIMEIRO_ANO_COM_META = 2024

# A distribuição por nível de proficiência só foi publicada em 2024.
ANOS_COM_DISTRIBUICAO_NIVEL = [2024]

PREFIXO_COLUNA_META = "meta_alfabetizacao_"

# Códigos conforme a tabela `dicionario` da própria fonte.
MAPA_REDE = {
    "0": "Total",
    "1": "Federal",
    "2": "Estadual",
    "3": "Municipal",
    "4": "Privada",
    "5": "Pública",
    "6": "Pública com Federal",
}

MAPA_REDE_DESCRICAO = {
    "0": "Total (Federal, Estadual, Municipal e Privada)",
    "1": "Federal",
    "2": "Estadual",
    "3": "Municipal",
    "4": "Privada",
    "5": "Pública (Estadual e Municipal)",
    "6": "Pública (Federal, Estadual e Municipal)",
}

# Metas usam texto; resultados usam código. Sem esta ponte não há join.
REDE_TEXTO_PARA_CODIGO = {
    "Municipal": "3",
    "Pública": "5",
    "Estadual": "2",
    "Federal": "1",
    "Privada": "4",
    "Total": "0",
}

# Metas municipais são da rede Municipal; as de UF e Brasil, da Pública.
REDE_INTEGRACAO_MUNICIPIO = "3"

CODIGO_UF_PARA_SIGLA = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA",
    "16": "AP", "17": "TO", "21": "MA", "22": "PI", "23": "CE",
    "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE",
    "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "42": "SC", "43": "RS", "50": "MS", "51": "MT",
    "52": "GO", "53": "DF",
}

REGIAO_POR_SIGLA = {
    "RO": "Norte", "AC": "Norte", "AM": "Norte", "RR": "Norte",
    "PA": "Norte", "AP": "Norte", "TO": "Norte",
    "MA": "Nordeste", "PI": "Nordeste", "CE": "Nordeste",
    "RN": "Nordeste", "PB": "Nordeste", "PE": "Nordeste",
    "AL": "Nordeste", "SE": "Nordeste", "BA": "Nordeste",
    "MG": "Sudeste", "ES": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "SC": "Sul", "RS": "Sul",
    "MS": "Centro-Oeste", "MT": "Centro-Oeste", "GO": "Centro-Oeste",
    "DF": "Centro-Oeste",
}

# O nome vem da pasta no S3: por isso o indicador por UF é `alfabetizacao`.
TABELAS_BRONZE = {
    "indicador_uf": "alfabetizacao",
    "indicador_municipio": "municipios",
    "aluno": "alunos",
    "meta_municipio": "metas_municipios",
    "meta_uf": "metas_uf",
    "meta_brasil": "metas_brasil",
}

COLUNAS_NIVEL = [f"proporcao_aluno_nivel_{i}" for i in range(9)]

# `alfabetizado` vale 0 para os 512.153 ausentes, não nulo: agregar sem
# filtrar trata ausência como reprovação.
PRESENTE = "1"
PROVA_PREENCHIDA = "1"

# A faixa imediatamente abaixo do corte é onde a intervenção pedagógica
# tem maior retorno marginal.
MARGEM_PROXIMIDADE = 50

# ---------------------------------------------------------------------------
# Schema explícito das saídas
#
# O schema desta camada é decisão, não inferência. Identificadores são
# string: código IBGE como número perde o zero à esquerda.
# ---------------------------------------------------------------------------

ESQUEMA_SILVER = {
    "dim_territorio": [
        ("id_municipio", "string"),
        ("codigo_uf", "string"),
        ("sigla_uf", "string"),
        ("regiao", "string"),
        ("nome_municipio", "string"),
    ],
    "dim_rede": [
        ("rede_codigo", "string"),
        ("rede_nome", "string"),
        ("rede_descricao", "string"),
    ],
    "fato_indicador_municipio": [
        ("ano", "int"),
        ("id_municipio", "string"),
        ("codigo_uf", "string"),
        ("sigla_uf", "string"),
        ("regiao", "string"),
        ("serie", "string"),
        ("rede_codigo", "string"),
        ("rede_nome", "string"),
        ("taxa_alfabetizacao", "double"),
        ("media_portugues", "double"),
        ("tem_distribuicao_nivel", "boolean"),
    ] + [(c, "double") for c in COLUNAS_NIVEL],
    "fato_indicador_uf": [
        ("ano", "int"),
        ("sigla_uf", "string"),
        ("regiao", "string"),
        ("serie", "string"),
        ("rede_codigo", "string"),
        ("rede_nome", "string"),
        ("taxa_alfabetizacao", "double"),
        ("media_portugues", "double"),
        ("tem_distribuicao_nivel", "boolean"),
    ] + [(c, "double") for c in COLUNAS_NIVEL],
    "fato_aluno": [
        ("ano", "int"),
        ("id_aluno", "string"),
        ("id_municipio", "string"),
        ("id_escola", "string"),
        ("codigo_uf", "string"),
        ("sigla_uf", "string"),
        ("regiao", "string"),
        ("serie", "string"),
        ("caderno", "string"),
        ("rede_codigo", "string"),
        ("rede_nome", "string"),
        ("presente", "boolean"),
        ("prova_preenchida", "boolean"),
        ("aluno_valido", "boolean"),
        ("alfabetizado", "boolean"),
        ("proficiencia", "double"),
        ("distancia_corte", "double"),
        ("faixa_proximidade", "string"),
        ("peso_aluno", "double"),
    ],
    "fato_meta": [
        ("nivel_territorial", "string"),
        ("safra", "int"),
        ("ano_meta", "int"),
        ("id_municipio", "string"),
        ("sigla_uf", "string"),
        ("rede_codigo", "string"),
        ("rede_nome", "string"),
        ("meta_alfabetizacao", "double"),
        ("taxa_alfabetizacao", "double"),
        ("percentual_participacao", "double"),
        ("nivel_alfabetizacao", "int"),
    ],
    "meta_vs_resultado": [
        ("ano", "int"),
        ("id_municipio", "string"),
        ("codigo_uf", "string"),
        ("sigla_uf", "string"),
        ("regiao", "string"),
        ("rede_codigo", "string"),
        ("rede_nome", "string"),
        ("taxa_alfabetizacao", "double"),
        ("media_portugues", "double"),
        ("meta_alfabetizacao", "double"),
        ("nivel_alfabetizacao", "int"),
        ("distancia_meta", "double"),
        ("atingiu_meta", "boolean"),
        ("situacao_meta", "string"),
    ],
}

SITUACAO_META = {
    "comparavel": "resultado e meta disponiveis",
    "ano_base": "ano-base do Compromisso, sem meta por definicao",
    "meta_nao_publicada": "municipio consta na tabela, meta do ano e nula",
    "municipio_sem_meta": "municipio ausente da tabela de metas",
}

SITUACOES_ANOMALAS = ["municipio_sem_meta", "meta_nao_publicada"]


# ===========================================================================
# Transformações
#
# Funções puras sobre DataFrame: testáveis com um SparkSession comum.
# ===========================================================================


def _mapa(dicionario: dict):
    """Converte um dict Python em expressão de mapa do Spark."""

    itens = []

    for chave, valor in dicionario.items():
        itens.append(F.lit(chave))
        itens.append(F.lit(valor))

    return F.create_map(*itens)


def traduzir_rede(df: DataFrame) -> DataFrame:
    """Acrescenta nome e descrição a partir do código da rede."""

    return (
        df.withColumn("rede_codigo", F.col("rede").cast("string"))
        .withColumn("rede_nome", _mapa(MAPA_REDE)[F.col("rede_codigo")])
        .withColumn("rede_descricao", _mapa(MAPA_REDE_DESCRICAO)[F.col("rede_codigo")])
        .drop("rede")
    )


def codificar_rede_das_metas(df: DataFrame) -> DataFrame:
    """Converte a rede textual das metas no código usado nos resultados."""

    return (
        df.withColumn("rede_codigo", _mapa(REDE_TEXTO_PARA_CODIGO)[F.col("rede")])
        .withColumn("rede_nome", _mapa(MAPA_REDE)[F.col("rede_codigo")])
        .drop("rede")
    )


def derivar_territorio(df: DataFrame) -> DataFrame:
    """Deriva UF e região dos dois primeiros dígitos do código IBGE."""

    return (
        df.withColumn("codigo_uf", F.substring(F.col("id_municipio"), 1, 2))
        .withColumn("sigla_uf", _mapa(CODIGO_UF_PARA_SIGLA)[F.col("codigo_uf")])
        .withColumn("regiao", _mapa(REGIAO_POR_SIGLA)[F.col("sigla_uf")])
    )


def marcar_distribuicao_nivel(df: DataFrame) -> DataFrame:
    """
    Sinaliza se a linha tem distribuição por nível preenchida.

    A ausência é estrutural — só 2024 foi publicado com esse detalhe.
    """

    presentes = [c for c in COLUNAS_NIVEL if c in df.columns]

    if not presentes:
        return df.withColumn("tem_distribuicao_nivel", F.lit(False))

    condicao = F.col(presentes[0]).isNotNull()

    for coluna in presentes[1:]:
        condicao = condicao | F.col(coluna).isNotNull()

    return df.withColumn("tem_distribuicao_nivel", condicao)


def construir_fato_indicador(df: DataFrame, com_municipio: bool) -> DataFrame:
    """Padroniza um fato de indicador, no grão de município ou de UF."""

    resultado = traduzir_rede(df)

    if com_municipio:
        resultado = derivar_territorio(resultado)
    else:
        resultado = resultado.withColumn(
            "regiao", _mapa(REGIAO_POR_SIGLA)[F.col("sigla_uf")]
        )

    return marcar_distribuicao_nivel(resultado)


def _alinhar(df: DataFrame, colunas: list[str]) -> DataFrame:
    """Garante que o DataFrame tenha todas as colunas, na mesma ordem."""

    for coluna in colunas:
        if coluna not in df.columns:
            df = df.withColumn(coluna, F.lit(None))

    return df.select(*colunas)


def desempilhar_metas(df: DataFrame, nivel: str) -> DataFrame:
    """
    Converte as metas de formato largo para longo.

    O campo `ano` da origem é a safra de publicação, não o ano da meta.
    """

    colunas_meta = [c for c in df.columns if c.startswith(PREFIXO_COLUNA_META)]

    pares = ", ".join(
        f"'{c.removeprefix(PREFIXO_COLUNA_META)}', `{c}`" for c in colunas_meta
    )

    manter = [c for c in df.columns if c not in colunas_meta]

    longo = df.select(
        *manter,
        F.expr(
            f"stack({len(colunas_meta)}, {pares}) as (ano_meta, meta_alfabetizacao)"
        ),
    )

    return (
        longo.withColumnRenamed("ano", "safra")
        .withColumn("ano_meta", F.col("ano_meta").cast("int"))
        .withColumn("nivel_territorial", F.lit(nivel))
    )


def construir_fato_meta(metas: dict[str, DataFrame]) -> DataFrame:
    """
    Une as metas dos três níveis e resolve as revisões entre safras.

    Para cada ente e ano-alvo prevalece a safra mais recente.
    """

    colunas = [
        "nivel_territorial", "safra", "ano_meta", "id_municipio", "sigla_uf",
        "rede_codigo", "rede_nome", "meta_alfabetizacao", "taxa_alfabetizacao",
        "percentual_participacao", "nivel_alfabetizacao",
    ]

    partes = []

    for nivel, df in metas.items():
        preparado = desempilhar_metas(codificar_rede_das_metas(df), nivel)
        partes.append(_alinhar(preparado, colunas))

    unido = partes[0]

    for parte in partes[1:]:
        unido = unido.unionByName(parte)

    janela = Window.partitionBy(
        "nivel_territorial", "id_municipio", "sigla_uf", "rede_codigo", "ano_meta"
    ).orderBy(F.col("safra").desc())

    return (
        unido.withColumn("ordem", F.row_number().over(janela))
        .filter(F.col("ordem") == 1)
        .drop("ordem")
    )


def integrar_meta_resultado(
    indicador: DataFrame, metas: DataFrame
) -> DataFrame:
    """
    Cruza resultado e meta no grão município x ano, rede Municipal.

    Nem toda ausência de meta é problema: 2023 não tem par por definição.
    Classificar as situações evita confundir ausência estrutural com lacuna.
    """

    esquerda = indicador.filter(F.col("rede_codigo") == REDE_INTEGRACAO_MUNICIPIO)

    direita = metas.filter(
        (F.col("nivel_territorial") == "municipio")
        & (F.col("rede_codigo") == REDE_INTEGRACAO_MUNICIPIO)
    ).select(
        F.col("id_municipio").alias("meta_id_municipio"),
        F.col("ano_meta"),
        F.col("meta_alfabetizacao"),
        F.col("nivel_alfabetizacao"),
    )

    # Separa falha de cobertura de meta não publicada para o ano
    com_registro = (
        direita.select(F.col("meta_id_municipio").alias("id_municipio"))
        .distinct()
        .withColumn("tem_registro_meta", F.lit(True))
    )

    integrado = (
        esquerda.join(
            direita,
            (esquerda["id_municipio"] == direita["meta_id_municipio"])
            & (esquerda["ano"] == direita["ano_meta"]),
            "left",
        )
        .drop("meta_id_municipio", "ano_meta")
        .join(com_registro, "id_municipio", "left")
    )

    # A ordem importa: ano-base tem precedência sobre as demais situações
    situacao = (
        F.when(F.col("ano") < PRIMEIRO_ANO_COM_META, F.lit("ano_base"))
        .when(F.col("meta_alfabetizacao").isNotNull(), F.lit("comparavel"))
        .when(F.col("tem_registro_meta").isNotNull(), F.lit("meta_nao_publicada"))
        .otherwise(F.lit("municipio_sem_meta"))
    )

    return (
        integrado.withColumn("situacao_meta", situacao)
        .withColumn(
            "distancia_meta",
            F.col("taxa_alfabetizacao") - F.col("meta_alfabetizacao"),
        )
        # Sem meta não é o mesmo que não atingiu: o nulo evita falso negativo
        .withColumn(
            "atingiu_meta",
            F.when(
                F.col("meta_alfabetizacao").isNull(), F.lit(None).cast("boolean")
            ).otherwise(F.col("distancia_meta") >= 0),
        )
        .drop("tem_registro_meta")
    )


def construir_fato_aluno(df: DataFrame) -> DataFrame:
    """
    Fato no grão do estudante, com os filtros e faixas que a agregação exige.

    Não filtra linhas, marca: descartar ausentes impediria medir a
    participação. Quem agrega usa `aluno_valido`.
    """

    resultado = traduzir_rede(df)
    resultado = derivar_territorio(resultado)

    presente = F.col("presenca") == PRESENTE
    preenchida = F.col("preenchimento_caderno") == PROVA_PREENCHIDA

    resultado = (
        resultado.withColumn("presente", presente)
        .withColumn("prova_preenchida", preenchida)
        .withColumn("aluno_valido", presente & preenchida)
        .withColumn("alfabetizado", F.col("alfabetizado") == "1")
        .withColumn(
            "distancia_corte",
            F.col("proficiencia") - F.lit(PONTO_CORTE_ALFABETIZACAO),
        )
    )

    # Proficiência nula é prova não realizada, não desempenho zero.
    faixa = (
        F.when(F.col("proficiencia").isNull(), F.lit("nao_avaliado"))
        .when(F.col("distancia_corte") < -MARGEM_PROXIMIDADE, F.lit("muito_abaixo"))
        .when(F.col("distancia_corte") < 0, F.lit("proximo_abaixo"))
        .when(F.col("distancia_corte") < MARGEM_PROXIMIDADE, F.lit("proximo_acima"))
        .otherwise(F.lit("muito_acima"))
    )

    return resultado.withColumn("faixa_proximidade", faixa)


def construir_dim_territorio(indicador: DataFrame) -> DataFrame:
    """
    Dimensão territorial derivada do código IBGE.

    `nome_municipio` fica vazia: não existe neste dataset. Declarada em vez
    de omitida para que a lacuna seja explícita.
    """

    return (
        indicador.select("id_municipio", "codigo_uf", "sigla_uf", "regiao")
        .distinct()
        .withColumn("nome_municipio", F.lit(None).cast("string"))
        .orderBy("id_municipio")
    )


def construir_dim_rede(spark: SparkSession) -> DataFrame:
    """Dimensão de rede, a partir do dicionário da fonte."""

    linhas = [
        (codigo, MAPA_REDE[codigo], MAPA_REDE_DESCRICAO[codigo])
        for codigo in sorted(MAPA_REDE)
    ]

    return spark.createDataFrame(
        linhas, ["rede_codigo", "rede_nome", "rede_descricao"]
    )


def aplicar_esquema(df: DataFrame, nome: str) -> DataFrame:
    """
    Projeta o DataFrame no schema declarado da Silver.

    Colunas fora do contrato são descartadas e os tipos, convertidos.
    """

    esquema = ESQUEMA_SILVER[nome]

    projecao = []

    for coluna, tipo in esquema:
        if coluna in df.columns:
            projecao.append(F.col(coluna).cast(tipo).alias(coluna))
        else:
            projecao.append(F.lit(None).cast(tipo).alias(coluna))

    return df.select(*projecao)


# ===========================================================================
# Orquestração
# ===========================================================================


def construir_silver(bronze: dict[str, DataFrame], spark: SparkSession) -> dict:
    """Executa a camada Silver inteira e devolve as tabelas resultantes."""

    fato_municipio = construir_fato_indicador(
        bronze["indicador_municipio"], com_municipio=True
    )

    fato_uf = construir_fato_indicador(bronze["indicador_uf"], com_municipio=False)

    fato_meta = construir_fato_meta(
        {
            "municipio": bronze["meta_municipio"],
            "uf": bronze["meta_uf"],
            "brasil": bronze["meta_brasil"],
        }
    )

    fato_aluno = construir_fato_aluno(bronze["aluno"])

    integrado = integrar_meta_resultado(fato_municipio, fato_meta)

    return {
        "fato_aluno": aplicar_esquema(fato_aluno, "fato_aluno"),
        "dim_territorio": aplicar_esquema(
            construir_dim_territorio(fato_municipio), "dim_territorio"
        ),
        "dim_rede": aplicar_esquema(construir_dim_rede(spark), "dim_rede"),
        "fato_indicador_municipio": aplicar_esquema(
            fato_municipio, "fato_indicador_municipio"
        ),
        "fato_indicador_uf": aplicar_esquema(fato_uf, "fato_indicador_uf"),
        "fato_meta": aplicar_esquema(fato_meta, "fato_meta"),
        "meta_vs_resultado": aplicar_esquema(integrado, "meta_vs_resultado"),
    }


DESTINOS = {
    "dim_territorio": "dimensoes/dim_territorio",
    "dim_rede": "dimensoes/dim_rede",
    "fato_indicador_municipio": "fatos/fato_indicador_municipio",
    "fato_indicador_uf": "fatos/fato_indicador_uf",
    "fato_aluno": "fatos/fato_aluno",
    "fato_meta": "fatos/fato_meta",
    "meta_vs_resultado": "integracao/meta_vs_resultado",
}


def gravar(tabelas: dict, base: str, logger) -> None:
    """Grava a Silver em Parquet."""

    for nome, caminho in DESTINOS.items():
        destino = f"{base}/silver/{caminho}/"

        # coalesce(1): sem isso o Spark gera dezenas de arquivos minúsculos
        tabelas[nome].coalesce(1).write.mode("overwrite").parquet(destino)

        logger(f"Gravado: {destino}")

    quarentena = tabelas["meta_vs_resultado"].filter(
        F.col("situacao_meta").isin(SITUACOES_ANOMALAS)
    )

    if quarentena.count() > 0:
        destino = f"{base}/silver/quarentena/"

        quarentena.withColumn(
            "motivo_quarentena", _mapa(SITUACAO_META)[F.col("situacao_meta")]
        ).withColumn("origem", F.lit("meta_vs_resultado")).coalesce(1).write.mode(
            "overwrite"
        ).parquet(destino)

        logger(f"Quarentena: {destino}")


def relatar(tabelas: dict, logger) -> None:
    """Registra volumes e a composição da integração."""

    logger("-" * 60)

    for nome in DESTINOS:
        logger(f"{nome:28} {tabelas[nome].count():>10,} linhas")

    logger("-" * 60)
    logger("Composicao da integracao:")

    composicao = (
        tabelas["meta_vs_resultado"]
        .groupBy("situacao_meta")
        .agg(
            F.count("*").alias("linhas"),
            F.countDistinct("id_municipio").alias("municipios"),
        )
        .orderBy(F.col("linhas").desc())
        .collect()
    )

    for linha in composicao:
        logger(
            f"  {linha['situacao_meta']:20} {linha['linhas']:>7,} linhas · "
            f"{linha['municipios']:>5,} municipios"
        )

    comparaveis = tabelas["meta_vs_resultado"].filter(
        F.col("situacao_meta") == "comparavel"
    )

    total = comparaveis.count()

    if total:
        atingiram = comparaveis.filter(F.col("atingiu_meta")).count()
        logger("-" * 60)
        logger(
            f"Atingiram a meta: {atingiram:,} de {total:,} "
            f"({atingiram / total * 100:.1f}%)"
        )


def ler_bronze(spark, glue_context, args) -> dict:
    """Lê a Bronze do Catalog ou direto do S3."""

    bronze = {}

    for chave, tabela in TABELAS_BRONZE.items():
        if args["FONTE"] == "catalog":
            bronze[chave] = glue_context.create_dynamic_frame.from_catalog(
                database=args["DATABASE_BRONZE"], table_name=tabela
            ).toDF()
        else:
            bronze[chave] = spark.read.parquet(
                f"s3://{args['BUCKET_ORIGEM']}/bronze/{tabela}/"
            )

    return bronze


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "BUCKET_ORIGEM",
            "BUCKET_DESTINO",
            "DATABASE_BRONZE",
            "ENV",
            "FONTE",
        ],
    )

    contexto = GlueContext(SparkContext.getOrCreate())
    spark = contexto.spark_session

    job = Job(contexto)
    job.init(args["JOB_NAME"], args)

    logger = contexto.get_logger().info

    logger("=" * 60)
    logger(f"CAMADA SILVER — ambiente {args['ENV']} — fonte {args['FONTE']}")
    logger("=" * 60)

    bronze = ler_bronze(spark, contexto, args)

    for nome, df in bronze.items():
        logger(f"Bronze: {nome:24} {df.count():>10,} linhas")

    tabelas = construir_silver(bronze, spark)

    gravar(tabelas, f"s3://{args['BUCKET_DESTINO']}", logger)
    relatar(tabelas, logger)

    logger("SILVER CONCLUIDA")

    job.commit()


if __name__ == "__main__":
    main()
