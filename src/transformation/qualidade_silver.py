"""
Qualidade da camada Silver — Glue Job (Spark).

Executa as regras Q1 a Q8 como Spark SQL sobre o Glue Catalog e grava o
relatório no S3. Regra bloqueante reprovada faz o Job falhar, o que
interrompe o Workflow e impede a promoção para a Gold.

As consultas rodam no próprio Spark, e não no Athena: um Job Spark que
delega a consulta a outro serviço mantém os workers ociosos enquanto
espera. Com o Catalog como metastore, `spark.sql` lê as mesmas tabelas
diretamente.

Parâmetros do Job:
    --JOB_NAME          nome do job (injetado pelo Glue)
    --BUCKET            bucket do data lake
    --DATABASE_BRONZE   database da Bronze no Catalog
    --DATABASE_SILVER   database da Silver no Catalog
"""

import sys
from dataclasses import dataclass
from datetime import datetime

import boto3

from awsglue.utils import getResolvedOptions

# ===========================================================================
# Constantes — espelham src/transformation/silver.py
# ===========================================================================

PONTO_CORTE = 743
DIGITOS_MUNICIPIO = 7
MUNICIPIOS_ESPERADOS = 5550
ANOS_ESPERADOS = 2
ANO_COM_DISTRIBUICAO = 2024

BLOQUEANTE = "bloqueante"
ALERTA = "alerta"

ATHENA_INTERVALO = 3
ATHENA_TENTATIVAS = 100


@dataclass
class Resultado:
    """Resultado de uma regra de validação."""

    id: str
    nome: str
    severidade: str
    esperado: str
    obtido: str
    aprovado: bool

    @property
    def bloqueia(self) -> bool:
        return self.severidade == BLOQUEANTE and not self.aprovado


class Consultor:
    """Executa Spark SQL sobre o Glue Catalog."""

    def __init__(self, spark, database_padrao: str):
        self.spark = spark
        self.database_padrao = database_padrao

    def _qualificar(self, sql: str, database: str) -> str:
        """Fixa o database da sessão antes de consultar."""

        self.spark.sql(f"USE {database or self.database_padrao}")

        return sql

    def linha(self, sql: str, database: str = None) -> list:
        """Primeira linha do resultado, com os valores como texto."""

        resultado = self.spark.sql(self._qualificar(sql, database)).collect()

        if not resultado:
            return []

        return [str(valor) for valor in resultado[0]]

    def valor(self, sql: str, database: str = None) -> str:
        """Primeiro valor da primeira linha."""

        dados = self.linha(sql, database)

        return dados[0] if dados else None

    def tabela(self, sql: str, database: str = None) -> list:
        """Todas as linhas do resultado."""

        return [
            [str(valor) for valor in linha]
            for linha in self.spark.sql(self._qualificar(sql, database)).collect()
        ]


# ===========================================================================
# Regras
# ===========================================================================


def q1_integridade_referencial(consultor: Consultor) -> Resultado:
    """Todo município da integração precisa existir na dimensão."""

    orfaos = consultor.valor("""
        SELECT COUNT(*)
        FROM meta_vs_resultado m
        LEFT JOIN dim_territorio d ON m.id_municipio = d.id_municipio
        WHERE d.id_municipio IS NULL
    """)

    return Resultado(
        id="Q1",
        nome="Integridade referencial de id_municipio",
        severidade=BLOQUEANTE,
        esperado="0 municipios orfaos",
        obtido=f"{orfaos} orfaos",
        aprovado=(orfaos == "0"),
    )


def q2_codigos_como_texto(consultor: Consultor, glue, database: str) -> Resultado:
    """
    Identificadores devem ser texto, com 7 dígitos preservados.

    O tipo vem do Catalog; o comprimento, dos dados. Código IBGE tratado
    como número perde o zero à esquerda e o join falha sem lançar erro.
    """

    tabela = glue.get_table(DatabaseName=database, Name="fato_indicador_municipio")

    tipo = next(
        (
            coluna["Type"]
            for coluna in tabela["Table"]["StorageDescriptor"]["Columns"]
            if coluna["Name"] == "id_municipio"
        ),
        "AUSENTE",
    )

    fora = consultor.valor(f"""
        SELECT COUNT(*)
        FROM fato_indicador_municipio
        WHERE length(id_municipio) <> {DIGITOS_MUNICIPIO}
    """)

    return Resultado(
        id="Q2",
        nome=f"Identificadores como texto de {DIGITOS_MUNICIPIO} digitos",
        severidade=BLOQUEANTE,
        esperado=f"tipo string, todos com {DIGITOS_MUNICIPIO} digitos",
        obtido=f"tipo {tipo}, {fora} fora do padrao",
        aprovado=(tipo == "string" and fora == "0"),
    )


def q3_unicidade_chaves(consultor: Consultor) -> Resultado:
    """A chave natural do fato precisa ser única."""

    duplicadas = consultor.valor("""
        SELECT COUNT(*) FROM (
          SELECT ano, id_municipio, rede_codigo
          FROM fato_indicador_municipio
          GROUP BY 1, 2, 3
          HAVING COUNT(*) > 1
        )
    """)

    return Resultado(
        id="Q3",
        nome="Unicidade da chave natural",
        severidade=BLOQUEANTE,
        esperado="nenhuma duplicata",
        obtido=f"{duplicadas} chaves duplicadas",
        aprovado=(duplicadas == "0"),
    )


def q4_vinculo_territorial(consultor: Consultor) -> Resultado:
    """Todo município precisa ter UF e região derivadas."""

    sem_vinculo = consultor.valor("""
        SELECT COUNT(*)
        FROM dim_territorio
        WHERE sigla_uf IS NULL OR regiao IS NULL
    """)

    return Resultado(
        id="Q4",
        nome="Vinculo territorial derivado do codigo IBGE",
        severidade=BLOQUEANTE,
        esperado="todos com UF e regiao",
        obtido=f"{sem_vinculo} sem vinculo",
        aprovado=(sem_vinculo == "0"),
    )


def q5_ponto_de_corte(consultor: Consultor, database_bronze: str) -> Resultado:
    """
    alfabetizado deve equivaler a proficiencia >= 743.

    Alunos sem proficiência não fizeram a prova e ficam fora da conta.
    """

    divergentes = consultor.valor(f"""
        SELECT COUNT(*)
        FROM alunos
        WHERE proficiencia IS NOT NULL
          AND (
            (proficiencia >= {PONTO_CORTE} AND alfabetizado = '0')
            OR (proficiencia < {PONTO_CORTE} AND alfabetizado = '1')
          )
    """, database_bronze)

    total = consultor.valor(
        "SELECT COUNT(*) FROM alunos WHERE proficiencia IS NOT NULL",
        database_bronze,
    )

    return Resultado(
        id="Q5",
        nome=f"Coerencia com o ponto de corte {PONTO_CORTE}",
        severidade=BLOQUEANTE,
        esperado="0 divergencias",
        obtido=f"{divergentes} em {total} registros",
        aprovado=(divergentes == "0"),
    )


def q6_cobertura(consultor: Consultor) -> Resultado:
    """A cobertura temporal e territorial deve seguir o esperado."""

    dados = consultor.linha("""
        SELECT COUNT(DISTINCT id_municipio), COUNT(DISTINCT ano)
        FROM fato_indicador_municipio
    """)

    municipios, anos = dados[0], dados[1]

    conforme = (
        municipios == str(MUNICIPIOS_ESPERADOS) and anos == str(ANOS_ESPERADOS)
    )

    return Resultado(
        id="Q6",
        nome="Cobertura temporal e territorial",
        severidade=ALERTA,
        esperado=f"{MUNICIPIOS_ESPERADOS} municipios, {ANOS_ESPERADOS} anos",
        obtido=f"{municipios} municipios, {anos} anos",
        aprovado=conforme,
    )


def q7_nulos_estruturais(consultor: Consultor) -> Resultado:
    """
    A distribuição por nível só existe em 2024.

    Valor fora desse padrão não é erro de dado: significa que a premissa
    sobre a fonte mudou, e a regra precisa ser revisitada.
    """

    fora = consultor.valor(f"""
        SELECT COUNT(*)
        FROM fato_indicador_municipio
        WHERE (ano = {ANO_COM_DISTRIBUICAO} AND NOT tem_distribuicao_nivel)
           OR (ano <> {ANO_COM_DISTRIBUICAO} AND tem_distribuicao_nivel)
    """)

    return Resultado(
        id="Q7",
        nome="Nulos estruturais na distribuicao por nivel",
        severidade=ALERTA,
        esperado=f"distribuicao apenas em {ANO_COM_DISTRIBUICAO}",
        obtido=f"{fora} linhas fora do padrao",
        aprovado=(fora == "0"),
    )


def q8_conservacao_volume(consultor: Consultor, database_bronze: str) -> Resultado:
    """
    Nenhum registro pode desaparecer entre camadas.

    Perda silenciosa é o erro mais perigoso da pipeline: não lança exceção
    e produz números plausíveis.
    """

    bronze = consultor.valor("SELECT COUNT(*) FROM municipios", database_bronze)
    silver = consultor.valor("SELECT COUNT(*) FROM fato_indicador_municipio")

    return Resultado(
        id="Q8",
        nome="Conservacao de volume entre camadas",
        severidade=BLOQUEANTE,
        esperado="bronze = silver",
        obtido=f"bronze {bronze}, silver {silver}",
        aprovado=(bronze == silver),
    )


# ===========================================================================
# Relatório
# ===========================================================================


def montar_relatorio(
    resultados: list,
    composicao: list,
    atingiram: str,
    comparaveis: str,
    database: str,
) -> str:
    """Monta o relatório em Markdown."""

    aprovadas = sum(1 for r in resultados if r.aprovado)
    reprovadas = len(resultados) - aprovadas
    bloqueios = sum(1 for r in resultados if r.bloqueia)

    situacao = "REPROVADA" if bloqueios else "APROVADA"

    percentual = (
        f"{int(atingiram) / int(comparaveis) * 100:.1f}"
        if comparaveis and int(comparaveis) > 0
        else "0.0"
    )

    linhas = [
        "# Relatorio de qualidade — camada Silver",
        "",
        f"**Execucao:** {datetime.now().strftime('%d/%m/%Y %H:%M')}  ",
        f"**Origem:** Spark SQL sobre o Glue Catalog (`{database}`)  ",
        f"**Situacao:** {situacao}  ",
        f"**Regras:** {aprovadas} aprovadas · {reprovadas} reprovadas · "
        f"{bloqueios} bloqueios",
        "",
        "---",
        "",
        "## Resultados",
        "",
        "| Regra | Severidade | Situacao | Esperado | Obtido |",
        "|---|---|---|---|---|",
    ]

    for r in resultados:
        marcador = "aprovado" if r.aprovado else "reprovado"
        linhas.append(
            f"| **{r.id}** {r.nome} | {r.severidade} | {marcador} | "
            f"{r.esperado} | {r.obtido} |"
        )

    linhas += [
        "",
        "## Composicao da integracao",
        "",
        "| Situacao | Linhas | Municipios |",
        "|---|---:|---:|",
    ]

    for situacao_meta, total, municipios in composicao:
        linhas.append(f"| {situacao_meta} | {total} | {municipios} |")

    linhas += [
        "",
        "## Resultado analitico",
        "",
        f"Entre os municipios com meta publicada para o ano, "
        f"**{atingiram} de {comparaveis} ({percentual}%)** atingiram a meta "
        f"na rede Municipal.",
        "",
        "---",
        "",
        "Gerado pelo Glue Job de qualidade. As consultas sao Spark SQL "
        "sobre o Catalog e podem ser reexecutadas por qualquer pessoa com "
        "acesso a ele — inclusive no Athena, que le o mesmo metastore.",
        "",
    ]

    return "\n".join(linhas)


# ===========================================================================
# Execução
# ===========================================================================


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "BUCKET", "DATABASE_BRONZE", "DATABASE_SILVER"],
    )

    bucket = args["BUCKET"]
    database_bronze = args["DATABASE_BRONZE"]
    database_silver = args["DATABASE_SILVER"]

    contexto = GlueContext(SparkContext.getOrCreate())
    spark = contexto.spark_session

    job = Job(contexto)
    job.init(args["JOB_NAME"], args)

    consultor = Consultor(spark, database_silver)

    glue = boto3.client("glue")
    s3 = boto3.client("s3")

    print("=" * 60)
    print("QUALIDADE DA CAMADA SILVER")
    print("=" * 60)

    resultados = [
        q1_integridade_referencial(consultor),
        q2_codigos_como_texto(consultor, glue, database_silver),
        q3_unicidade_chaves(consultor),
        q4_vinculo_territorial(consultor),
        q5_ponto_de_corte(consultor, database_bronze),
        q6_cobertura(consultor),
        q7_nulos_estruturais(consultor),
        q8_conservacao_volume(consultor, database_bronze),
    ]

    for r in resultados:
        marcador = "OK   " if r.aprovado else "FALHA"
        print(f"{marcador} {r.id} · {r.nome} — {r.obtido}")

    # As consultas de composicao e resultado usam o database da Silver
    consultor.spark.sql(f"USE {database_silver}")

    composicao = consultor.tabela("""
        SELECT situacao_meta, COUNT(*), COUNT(DISTINCT id_municipio)
        FROM meta_vs_resultado
        GROUP BY situacao_meta
        ORDER BY 2 DESC
    """)

    print("-" * 60)
    print("Composicao da integracao:")

    for situacao, total, municipios in composicao:
        print(f"  {situacao:22} {total:>8} linhas · {municipios:>6} municipios")

    analitico = consultor.linha("""
        SELECT
          COUNT(*) FILTER (WHERE atingiu_meta) AS atingiram,
          COUNT(*) AS comparaveis
        FROM meta_vs_resultado
        WHERE situacao_meta = 'comparavel'
    """)

    atingiram, comparaveis = analitico[0], analitico[1]

    print("-" * 60)
    print(f"Atingiram a meta: {atingiram} de {comparaveis}")

    relatorio = montar_relatorio(
        resultados, composicao, atingiram, comparaveis, database_silver
    )

    carimbo = datetime.now().strftime("%Y%m%d-%H%M")
    chave = f"quality/reports/relatorio-silver-{carimbo}.md"

    s3.put_object(
        Bucket=bucket,
        Key=chave,
        Body=relatorio.encode("utf-8"),
        ContentType="text/markdown",
    )

    print("-" * 60)
    print(f"Relatorio: s3://{bucket}/{chave}")

    bloqueios = [r for r in resultados if r.bloqueia]

    if bloqueios:
        nomes = ", ".join(r.id for r in bloqueios)
        raise RuntimeError(
            f"{len(bloqueios)} regra(s) bloqueante(s) reprovada(s): {nomes}"
        )

    print("Todas as regras bloqueantes aprovadas")

    job.commit()


if __name__ == "__main__":
    main()
