// ---------------------------------------------------------------------------
// Orquestração — Glue Workflow
//
// Encadeia crawler, transformação e qualidade dentro da própria AWS. 
//
// Fluxo:
//   trigger sob demanda
//     -> crawler da Bronze
//        -> (SUCCEEDED) job da Silver
//           -> (SUCCEEDED) job de qualidade
//
// Cada etapa só dispara se a anterior teve sucesso. O job de qualidade
// levanta exceção quando uma regra bloqueante reprova, o que o faz falhar
// e interrompe o fluxo — a Gold não é gerada sobre uma Silver inválida.
//
// Glue Workflow foi escolhido em vez de Step Functions ou MWAA: encadeia
// crawlers e jobs nativamente, não exige infraestrutura adicional e não
// tem custo próprio. 
// ---------------------------------------------------------------------------

resource "aws_glue_workflow" "silver" {
  name        = "${var.prefixo}_workflow_silver"
  description = "Cataloga a Bronze, constroi a Silver e valida a qualidade"

  // Disponibiliza os parâmetros para todas as etapas do fluxo, evitando
  // repetir bucket e database em cada job.
  default_run_properties = {
    bucket          = var.bucket
    database_bronze = aws_glue_catalog_database.camada["bronze"].name
    database_silver = aws_glue_catalog_database.camada["silver"].name
  }

  tags = merge(local.tags_comuns, { Layer = "orquestracao" })
}

// ---------------------------------------------------------------------------
// Job de qualidade — Python Shell
// ---------------------------------------------------------------------------

resource "aws_s3_object" "script_qualidade" {
  bucket = var.bucket
  key    = "scripts/qualidade_silver.py"
  source = var.caminho_script_qualidade
  etag   = filemd5(var.caminho_script_qualidade)
}

resource "aws_glue_job" "qualidade" {
  name        = "${var.prefixo}_job_qualidade"
  description = "Validacoes Q1-Q8 da camada Silver em Spark SQL"
  role_arn    = var.role_glue

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  // Timeout menor que o do Job da Silver: são consultas agregadas sobre
  // tabelas de poucos MB, e o tempo dominante é a inicialização do Spark.
  timeout = 15

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket}/${aws_s3_object.script_qualidade.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--BUCKET"          = var.bucket
    "--DATABASE_BRONZE" = aws_glue_catalog_database.camada["bronze"].name
    "--DATABASE_SILVER" = aws_glue_catalog_database.camada["silver"].name

    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"

    "--enable-glue-datacatalog" = "true"

    // Reconstrói a avaliação inteira a cada execução, como a Silver.
    "--job-bookmark-option" = "job-bookmark-disable"
  }

  // As tabelas precisam existir no Catalog antes de o Job consultá-las.
  depends_on = [aws_glue_catalog_table.silver]

  tags = merge(local.tags_comuns, { Layer = "qualidade" })
}

// ---------------------------------------------------------------------------
// Gatilhos
// ---------------------------------------------------------------------------

// Início do fluxo. ON_DEMAND em vez de SCHEDULED porque os dados de
// alfabetização são anuais: agendamento diário dispararia execuções que
// reprocessam o mesmo dado. Para agendar, troque o type por SCHEDULED e
// informe um schedule em expressão cron.
resource "aws_glue_trigger" "inicio" {
  name          = "${var.prefixo}_trigger_inicio"
  description   = "Inicia o fluxo catalogando a Bronze"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.silver.name

  actions {
    crawler_name = aws_glue_crawler.bronze.name
  }

  tags = local.tags_comuns
}

resource "aws_glue_trigger" "bronze_para_silver" {
  name          = "${var.prefixo}_trigger_silver"
  description   = "Constroi a Silver quando a Bronze estiver catalogada"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.silver.name

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.bronze.name
      crawl_state  = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.silver.name
  }

  tags = local.tags_comuns
}

resource "aws_glue_trigger" "silver_para_qualidade" {
  name          = "${var.prefixo}_trigger_qualidade"
  description   = "Valida a Silver depois de construida"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.silver.name

  predicate {
    conditions {
      job_name = aws_glue_job.silver.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.qualidade.name
  }

  tags = local.tags_comuns
}
