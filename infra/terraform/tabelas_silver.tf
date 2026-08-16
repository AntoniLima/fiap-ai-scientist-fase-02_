// ---------------------------------------------------------------------------
// Tabelas da camada Silver — schema explícito
//
// Diferente da Bronze, a Silver não usa Crawler. O schema desta camada é
// explícito: `atingiu_meta` é boolean porque "sem meta" não
// é "não atingiu"; `id_municipio` é string porque código IBGE não é número.
// Deixar um Crawler inferir isso terceirizaria a decisão para um palpite
// sobre os dados de uma execução específica.
//
// As definições abaixo espelham ESQUEMA_SILVER em
// src/transformation/silver.py. Se uma mudar, a outra precisa mudar junto.
// ---------------------------------------------------------------------------

locals {
  // As nove proporções por nível de proficiência. Só têm valor em 2024 —
  // ausência estrutural, sinalizada por tem_distribuicao_nivel.
  niveis_proficiencia = [
    for i in range(9) : {
      name = "proporcao_aluno_nivel_${i}"
      type = "double"
    }
  ]

  colunas_indicador_comuns = [
    { name = "serie", type = "string" },
    { name = "rede_codigo", type = "string" },
    { name = "rede_nome", type = "string" },
    { name = "taxa_alfabetizacao", type = "double" },
    { name = "media_portugues", type = "double" },
    { name = "tem_distribuicao_nivel", type = "boolean" },
  ]

  colunas_meta_vs_resultado = [
    { name = "ano", type = "int" },
    { name = "id_municipio", type = "string" },
    { name = "codigo_uf", type = "string" },
    { name = "sigla_uf", type = "string" },
    { name = "regiao", type = "string" },
    { name = "rede_codigo", type = "string" },
    { name = "rede_nome", type = "string" },
    { name = "taxa_alfabetizacao", type = "double" },
    { name = "media_portugues", type = "double" },
    { name = "meta_alfabetizacao", type = "double" },
    { name = "nivel_alfabetizacao", type = "int" },
    { name = "distancia_meta", type = "double" },
    { name = "atingiu_meta", type = "boolean" },
    { name = "situacao_meta", type = "string" },
  ]

  tabelas_silver = {
    dim_territorio = {
      location = "dimensoes/dim_territorio"
      columns = [
        { name = "id_municipio", type = "string" },
        { name = "codigo_uf", type = "string" },
        { name = "sigla_uf", type = "string" },
        { name = "regiao", type = "string" },
        // O nome do município não existe neste dataset. A coluna é
        // declarada em vez de omitida, para que a lacuna fique explícita.
        { name = "nome_municipio", type = "string" },
      ]
    }

    dim_rede = {
      location = "dimensoes/dim_rede"
      columns = [
        { name = "rede_codigo", type = "string" },
        { name = "rede_nome", type = "string" },
        { name = "rede_descricao", type = "string" },
      ]
    }

    fato_indicador_municipio = {
      location = "fatos/fato_indicador_municipio"
      columns = concat(
        [
          { name = "ano", type = "int" },
          { name = "id_municipio", type = "string" },
          { name = "codigo_uf", type = "string" },
          { name = "sigla_uf", type = "string" },
          { name = "regiao", type = "string" },
        ],
        local.colunas_indicador_comuns,
        local.niveis_proficiencia,
      )
    }

    fato_indicador_uf = {
      location = "fatos/fato_indicador_uf"
      columns = concat(
        [
          { name = "ano", type = "int" },
          { name = "sigla_uf", type = "string" },
          { name = "regiao", type = "string" },
        ],
        local.colunas_indicador_comuns,
        local.niveis_proficiencia,
      )
    }

    // Grao do estudante: 3,9 milhoes de linhas. Nao filtra ausentes —
    // marca com aluno_valido, para que a taxa de participacao continue
    // mensuravel. Quem agrega decide o recorte.
    fato_aluno = {
      location = "fatos/fato_aluno"
      columns = [
        { name = "ano", type = "int" },
        { name = "id_aluno", type = "string" },
        { name = "id_municipio", type = "string" },
        { name = "id_escola", type = "string" },
        { name = "codigo_uf", type = "string" },
        { name = "sigla_uf", type = "string" },
        { name = "regiao", type = "string" },
        { name = "serie", type = "string" },
        { name = "caderno", type = "string" },
        { name = "rede_codigo", type = "string" },
        { name = "rede_nome", type = "string" },
        { name = "presente", type = "boolean" },
        { name = "prova_preenchida", type = "boolean" },
        // Filtro obrigatorio antes de qualquer agregacao: a coluna
        // alfabetizado vale false para quem nao fez a prova.
        { name = "aluno_valido", type = "boolean" },
        { name = "alfabetizado", type = "boolean" },
        { name = "proficiencia", type = "double" },
        { name = "distancia_corte", type = "double" },
        // Faixa em relacao ao corte de 743. proximo_abaixo e o grupo com
        // maior retorno marginal de intervencao pedagogica.
        { name = "faixa_proximidade", type = "string" },
        { name = "peso_aluno", type = "double" },
      ]
    }

    fato_meta = {
      location = "fatos/fato_meta"
      columns = [
        { name = "nivel_territorial", type = "string" },
        // safra é o ano de publicação; ano_meta é o ano-alvo. Confundir
        // os dois produz comparações sem sentido.
        { name = "safra", type = "int" },
        { name = "ano_meta", type = "int" },
        { name = "id_municipio", type = "string" },
        { name = "sigla_uf", type = "string" },
        { name = "rede_codigo", type = "string" },
        { name = "rede_nome", type = "string" },
        { name = "meta_alfabetizacao", type = "double" },
        { name = "taxa_alfabetizacao", type = "double" },
        { name = "percentual_participacao", type = "double" },
        { name = "nivel_alfabetizacao", type = "int" },
      ]
    }

    meta_vs_resultado = {
      location = "integracao/meta_vs_resultado"
      columns  = local.colunas_meta_vs_resultado
    }

    quarentena = {
      location = "quarentena"
      columns = concat(
        local.colunas_meta_vs_resultado,
        [
          { name = "motivo_quarentena", type = "string" },
          { name = "origem", type = "string" },
        ],
      )
    }
  }
}

resource "aws_glue_catalog_table" "silver" {
  for_each = local.tabelas_silver

  name          = each.key
  database_name = aws_glue_catalog_database.camada["silver"].name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_destino}/silver/${each.value.location}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = each.value.columns

      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }

  // As tabelas descrevem a saída do Job. Declarar a dependência garante
  // que o Job exista antes — não que já tenha rodado, o que é ação e não
  // estado.
  depends_on = [aws_glue_job.silver]
}
