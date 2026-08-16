// ---------------------------------------------------------------------------
// Infraestrutura AWS — Glue Catalog e Crawler
//
// Uso:
//   cd infra/terraform
//   terraform init
//   terraform plan
//   terraform apply
//   terraform destroy
//
// ---------------------------------------------------------------------------

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  // Backend local. Em ambiente persistente o state ficaria em S3 com
  // travamento em DynamoDB — aqui não faz sentido, porque o laboratório
  // do AWS Academy descarta os recursos entre sessões.
  backend "local" {
    path = "terraform.tfstate"
  }
}

provider "aws" {
  region = var.regiao

  // Credenciais vêm de ~/.aws/credentials, incluindo o session token do
  // AWS Academy. Nenhuma credencial é declarada aqui.
}

// ---------------------------------------------------------------------------
// Databases — um por camada da arquitetura medalhão
// ---------------------------------------------------------------------------

locals {
  // Vazio cai no bucket principal: um bucket com camadas em prefixos e a
  // decisao atual do projeto, mas a separacao fica declarada.
  bucket_origem  = var.bucket_origem != "" ? var.bucket_origem : var.bucket
  bucket_destino = var.bucket_destino != "" ? var.bucket_destino : var.bucket

  camadas = {
    bronze = "Camada Bronze - dados brutos do INEP, fiel a origem"
    silver = "Camada Silver - dados limpos, padronizados e integrados"
    gold   = "Camada Gold - indicadores e datasets analiticos"
  }

  // Tags aplicadas a todos os recursos que as suportam. Layer alimenta o
  // rastreio de custo por camada no Cost Explorer, insumo direto da seção
  // de FinOps. ManagedBy sinaliza que o recurso é gerenciado por IaC e não
  // deve ser alterado pelo console — alteração manual gera divergência que
  // o próximo terraform apply desfaz sem avisar.
  //
   tags_comuns = {
    Environment = var.ambiente
    ManagedBy   = "terraform"
    Pipeline    = "alfabetizacao"
    Project     = "fiap-tech-challenge-fase02"
  }

  // Subpastas da Bronze. Cada uma tem schema próprio e vira uma tabela.
  // Include paths explícitos evitam que o Crawler tente unificar schemas
  // incompatíveis ao varrer a raiz de bronze/.
  pastas_bronze = [
    "alfabetizacao",
    "alunos",
    "dicionario",
    "metas_brasil",
    "metas_municipios",
    "metas_uf",
    "municipios",
  ]
}

resource "aws_glue_catalog_database" "camada" {
  for_each = local.camadas

  name        = "${var.prefixo}_${each.key}"
  description = each.value

  location_uri = "s3://${var.bucket}/${each.key}/"

  // each.key já vale bronze, silver ou gold — cada database recebe a
  // própria camada sem repetição.
  //
  tags = merge(local.tags_comuns, { Layer = each.key })
}

// ---------------------------------------------------------------------------
// Crawler da Bronze
// ---------------------------------------------------------------------------

resource "aws_glue_crawler" "bronze" {
  name          = "${var.prefixo}_crawler_bronze"
  description   = "Cataloga as sete tabelas da camada Bronze"
  role          = var.role_glue
  database_name = aws_glue_catalog_database.camada["bronze"].name

  dynamic "s3_target" {
    for_each = local.pastas_bronze

    content {
      path = "s3://${var.bucket}/bronze/${s3_target.value}/"
    }
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"

    // LOG em vez de DELETE_FROM_DATABASE: registra a remoção sem apagar a
    // tabela, evitando perder metadados por uma execução parcial.
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }

  tags = merge(local.tags_comuns, { Layer = "bronze" })
}

// ---------------------------------------------------------------------------
// Glue Job — camada Silver
// ---------------------------------------------------------------------------

// O Job aponta para um script no S3. Sem enviar o arquivo, o recurso seria
// criado apontando para um caminho inexistente — por isso o upload faz
// parte da declaração. O etag garante reenvio quando o código muda.
resource "aws_s3_object" "script_silver" {
  bucket = var.bucket
  key    = "scripts/silver.py"
  source = var.caminho_script_silver
  etag   = filemd5(var.caminho_script_silver)
}

resource "aws_glue_job" "silver" {
  name         = "${var.prefixo}_job_silver"
  description  = "Camada Silver - limpeza, padronizacao e integracao meta x resultado"
  role_arn     = var.role_glue
  glue_version = "4.0"

  // Volume da Bronze é de 68 MB. Dois workers G.1X são o mínimo do Spark
  // e já sobra capacidade — Auto Scaling não teria o que escalar.
  worker_type       = "G.1X"
  number_of_workers = 2

  // Timeout curto e proposital: o padrão de 48h deixaria um job travado
  // consumindo DPU por dois dias, e o orçamento do laboratório é limitado.
  timeout = 20

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket}/${aws_s3_object.script_silver.key}"
    python_version  = "3"
  }

  default_arguments = {
    // Origem e destino separados mesmo apontando hoje para o mesmo
    // bucket: a separação entre camadas é conceitual, e explicitá-la
    // permite mover uma camada sem tocar no código do Job.
    "--BUCKET_ORIGEM"   = local.bucket_origem
    "--BUCKET_DESTINO"  = local.bucket_destino
    "--DATABASE_BRONZE" = aws_glue_catalog_database.camada["bronze"].name
    "--ENV"             = var.ambiente
    "--FONTE"           = var.fonte_bronze

    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"

    // Bookmark desabilitado de propósito: a Silver é reconstruída inteira
    // a cada execução. Com bookmark ligado, reprocessar após corrigir uma
    // regra exigiria reset — e o job "rodaria com sucesso" sem atualizar
    // nada.
    "--job-bookmark-option" = "job-bookmark-disable"
  }

  tags = merge(local.tags_comuns, { Layer = "silver" })

  depends_on = [aws_glue_crawler.bronze]
}
