#!/usr/bin/env bash
#
# Executa consultas analiticas sobre a camada Silver, no Athena.
#
# As consultas estão em sql/silver/ como arquivos versionados. 
#
# O resultado e salvo em sql/resultados/ como CSV, com carimbo de data.
#
# Uso:
#   bash scripts/consultar.sh                        # lista as consultas
#   bash scripts/consultar.sh distribuicao_por_faixa # executa uma
#   bash scripts/consultar.sh --todas                # executa todas

set -euo pipefail

export MSYS_NO_PATHCONV=1

PREFIXO="${PREFIXO:-alfabetizacao}"
BUCKET="${BUCKET:-fiap-ai-scientist-fase-02}"
REGIAO="${AWS_REGION:-us-east-1}"

DATABASE="${PREFIXO}_silver"

CONSULTAS_DIR="sql/silver"
RESULTADOS_DIR="sql/resultados"

ATHENA_SAIDA="s3://${BUCKET}/athena-results/"

INTERVALO=3
MAX_TENTATIVAS=60

info()  { echo "[INFO]  $*"; }
erro()  { echo "[ERRO]  $*" >&2; }

separador() { printf '%.0s-' {1..70}; echo; }

listar() {
  separador
  info "Consultas disponiveis em ${CONSULTAS_DIR}:"
  echo

  for arquivo in "$CONSULTAS_DIR"/*.sql; do
    local nome
    nome=$(basename "$arquivo" .sql)

    # Primeira linha de comentario serve de descricao
    local descricao
    descricao=$(head -1 "$arquivo" | sed 's/^-- *//')

    printf '  %-28s %s\n' "$nome" "$descricao"
  done

  echo
  info "Uso: bash scripts/consultar.sh <nome>"
  separador
}

executar() {
  local nome="$1"
  local arquivo="${CONSULTAS_DIR}/${nome}.sql"

  if [ ! -f "$arquivo" ]; then
    erro "Consulta nao encontrada: ${arquivo}"
    return 1
  fi

  separador
  info "Executando ${nome}"
  echo

  local sql
  sql=$(cat "$arquivo")

  local execucao
  execucao=$(aws athena start-query-execution \
    --region "$REGIAO" \
    --query-string "$sql" \
    --query-execution-context "Database=${DATABASE}" \
    --result-configuration "OutputLocation=${ATHENA_SAIDA}" \
    --query 'QueryExecutionId' \
    --output text)

  local tentativa=0
  local estado=""

  while [ "$tentativa" -lt "$MAX_TENTATIVAS" ]; do
    estado=$(aws athena get-query-execution \
      --region "$REGIAO" \
      --query-execution-id "$execucao" \
      --query 'QueryExecution.Status.State' \
      --output text)

    case "$estado" in
      SUCCEEDED) break ;;
      FAILED|CANCELLED)
        erro "Consulta ${estado}:"
        aws athena get-query-execution \
          --region "$REGIAO" \
          --query-execution-id "$execucao" \
          --query 'QueryExecution.Status.StateChangeReason' \
          --output text >&2
        return 1
        ;;
    esac

    sleep "$INTERVALO"
    tentativa=$((tentativa + 1))
  done

  if [ "$estado" != "SUCCEEDED" ]; then
    erro "Consulta nao concluiu no tempo previsto"
    return 1
  fi

  mkdir -p "$RESULTADOS_DIR"

  local destino="${RESULTADOS_DIR}/${nome}-$(date +%Y%m%d-%H%M).csv"

  aws s3 cp "${ATHENA_SAIDA}${execucao}.csv" "$destino" \
    --region "$REGIAO" --only-show-errors

  local escaneado
  escaneado=$(aws athena get-query-execution \
    --region "$REGIAO" \
    --query-execution-id "$execucao" \
    --query 'QueryExecution.Statistics.DataScannedInBytes' \
    --output text)

  local mb
  mb=$(awk -v b="$escaneado" 'BEGIN { printf "%.2f", b / 1024 / 1024 }')

  cat "$destino"

  echo
  info "Salvo em ${destino}"
  info "Dados escaneados: ${mb} MB"
}

main() {
  if ! aws sts get-caller-identity > /dev/null 2>&1; then
    erro "Credenciais invalidas ou expiradas."
    erro "No AWS Academy, copie novamente em AWS Details > AWS CLI > Show."
    exit 1
  fi

  if [ $# -eq 0 ]; then
    listar
    exit 0
  fi

  if [ "$1" = "--todas" ]; then
    for arquivo in "$CONSULTAS_DIR"/*.sql; do
      executar "$(basename "$arquivo" .sql)"
    done
    separador
    exit 0
  fi

  executar "$1"
  separador
}

main "$@"
