#!/usr/bin/env bash
#
# Executa o Crawler da camada Bronze e valida o resultado.
#
# O Crawler é criado pelo Terraform. Este script apenas o executa e
# verifica a saída — executar é ação, não estado, e por isso não cabe no
# modelo declarativo.
#
# A verificação de tipagem no fim não é formalidade: código IBGE inferido
# como número perde o zero à esquerda, e o join territorial passa a falhar
# sem lançar erro (ADR-005).
#
# Uso:
#   bash infra/executar_crawler.sh
#
# Pré-requisito:
#   terraform apply

set -euo pipefail

export MSYS_NO_PATHCONV=1

PREFIXO="${PREFIXO:-alfabetizacao}"
REGIAO="${AWS_REGION:-us-east-1}"

DATABASE="${PREFIXO}_bronze"
CRAWLER="${PREFIXO}_crawler_bronze"

TABELAS_ESPERADAS=7
TABELAS_COM_MUNICIPIO=("municipios" "alunos" "metas_municipios")

INTERVALO=10
MAX_TENTATIVAS=60

info()  { echo "[INFO]  $*"; }
aviso() { echo "[AVISO] $*"; }
erro()  { echo "[ERRO]  $*" >&2; }

separador() { printf '%.0s-' {1..70}; echo; }

verificar_pre_requisitos() {
  if ! aws sts get-caller-identity > /dev/null 2>&1; then
    erro "Credenciais invalidas ou expiradas."
    erro "No AWS Academy, copie novamente em AWS Details > AWS CLI > Show."
    exit 1
  fi

  if ! aws glue get-crawler --name "$CRAWLER" --region "$REGIAO" > /dev/null 2>&1; then
    erro "Crawler ${CRAWLER} nao encontrado."
    erro "Execute antes: cd infra/terraform && terraform apply"
    exit 1
  fi

  info "Crawler ${CRAWLER} encontrado"
}

executar() {
  info "Iniciando execucao..."

  aws glue start-crawler --name "$CRAWLER" --region "$REGIAO"

  local tentativa=0
  local estado=""

  while [ "$tentativa" -lt "$MAX_TENTATIVAS" ]; do
    estado=$(aws glue get-crawler \
      --name "$CRAWLER" --region "$REGIAO" \
      --query 'Crawler.State' --output text)

    if [ "$estado" = "READY" ] && [ "$tentativa" -gt 0 ]; then
      break
    fi

    printf '.'
    sleep "$INTERVALO"
    tentativa=$((tentativa + 1))
  done

  echo

  if [ "$estado" != "READY" ]; then
    erro "Nao concluiu em $((MAX_TENTATIVAS * INTERVALO))s (estado: ${estado})"
    exit 1
  fi

  local resultado
  resultado=$(aws glue get-crawler \
    --name "$CRAWLER" --region "$REGIAO" \
    --query 'Crawler.LastCrawl.Status' --output text)

  info "Concluido com status: ${resultado}"

  if [ "$resultado" != "SUCCEEDED" ]; then
    erro "O crawler nao concluiu com sucesso."
    aws glue get-crawler --name "$CRAWLER" --region "$REGIAO" \
      --query 'Crawler.LastCrawl.ErrorMessage' --output text
    exit 1
  fi
}

listar_tabelas() {
  separador
  info "Tabelas catalogadas em ${DATABASE}:"
  echo

  aws glue get-tables \
    --database-name "$DATABASE" --region "$REGIAO" \
    --query 'TableList[].[Name, length(StorageDescriptor.Columns), Parameters.recordCount]' \
    --output table

  local total
  total=$(aws glue get-tables \
    --database-name "$DATABASE" --region "$REGIAO" \
    --query 'length(TableList)' --output text)

  if [ "$total" -ne "$TABELAS_ESPERADAS" ]; then
    aviso "Esperadas ${TABELAS_ESPERADAS} tabelas, encontradas ${total}"
    aviso "O crawler pode ter unificado schemas — verifique manualmente"
  else
    info "${total} de ${TABELAS_ESPERADAS} tabelas catalogadas"
  fi
}

verificar_tipagem() {
  separador
  info "Verificando a tipagem de id_municipio (ADR-005)"
  echo

  local falhas=0

  for tabela in "${TABELAS_COM_MUNICIPIO[@]}"; do
    local tipo
    tipo=$(aws glue get-table \
      --database-name "$DATABASE" --name "$tabela" --region "$REGIAO" \
      --query 'Table.StorageDescriptor.Columns[?Name==`id_municipio`].Type | [0]' \
      --output text 2>/dev/null || echo "AUSENTE")

    if [ "$tipo" = "string" ]; then
      info "  ${tabela}.id_municipio = ${tipo}  OK"
    else
      erro "  ${tabela}.id_municipio = ${tipo}  <-- ATENCAO"
      falhas=$((falhas + 1))
    fi
  done

  echo

  if [ "$falhas" -gt 0 ]; then
    erro "O Crawler inferiu tipo numerico para id_municipio."
    erro "Codigo IBGE tratado como numero perde zeros a esquerda e o join"
    erro "territorial falha sem lancar erro. Corrija antes de rodar o Job."
    exit 1
  fi

  info "Tipagem preservada: o Parquet manteve string ate o Catalog"
}

main() {
  separador
  info "CRAWLER DA CAMADA BRONZE"
  separador

  verificar_pre_requisitos
  executar
  listar_tabelas
  verificar_tipagem

  separador
  info "Bronze catalogada. Proximo: bash infra/executar_job_silver.sh"
  separador
}

main "$@"
