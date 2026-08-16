#!/usr/bin/env bash
#
# Executa o Glue Job da camada Silver e verifica a saída.
#
# O Job, o upload do script e as tabelas do Catalog são criados pelo
# Terraform. Este script apenas dispara a execução, acompanha até o fim e
# reporta o consumo medido.
#
# Uso:
#   bash infra/executar_job_silver.sh
#
# Pré-requisitos:
#   terraform apply
#   bash infra/executar_crawler.sh 

set -euo pipefail

export MSYS_NO_PATHCONV=1

PREFIXO="${PREFIXO:-alfabetizacao}"
BUCKET="${BUCKET:-fiap-ai-scientist-fase-02}"
REGIAO="${AWS_REGION:-us-east-1}"

JOB="${PREFIXO}_job_silver"

INTERVALO=15
MAX_TENTATIVAS=80

info()  { echo "[INFO]  $*"; }
erro()  { echo "[ERRO]  $*" >&2; }

separador() { printf '%.0s-' {1..70}; echo; }

verificar_pre_requisitos() {
  if ! aws sts get-caller-identity > /dev/null 2>&1; then
    erro "Credenciais invalidas ou expiradas."
    exit 1
  fi

  if ! aws glue get-job --job-name "$JOB" --region "$REGIAO" > /dev/null 2>&1; then
    erro "Job ${JOB} nao encontrado."
    erro "Execute antes: cd infra/terraform && terraform apply"
    exit 1
  fi

  local fonte
  fonte=$(aws glue get-job --job-name "$JOB" --region "$REGIAO" \
    --query 'Job.DefaultArguments."--FONTE"' --output text)

  info "Job ${JOB} encontrado (fonte: ${fonte})"

  if [ "$fonte" = "catalog" ]; then
    local tabelas
    tabelas=$(aws glue get-tables \
      --database-name "${PREFIXO}_bronze" --region "$REGIAO" \
      --query 'length(TableList)' --output text 2>/dev/null || echo "0")

    if [ "$tabelas" -eq 0 ]; then
      erro "Nenhuma tabela em ${PREFIXO}_bronze."
      erro "Execute antes: bash infra/executar_crawler.sh"
      exit 1
    fi
  fi
}

executar() {
  info "Disparando execucao..."

  local execucao
  execucao=$(aws glue start-job-run \
    --job-name "$JOB" --region "$REGIAO" \
    --query 'JobRunId' --output text)

  info "JobRunId: ${execucao}"

  local tentativa=0
  local estado=""

  while [ "$tentativa" -lt "$MAX_TENTATIVAS" ]; do
    estado=$(aws glue get-job-run \
      --job-name "$JOB" --run-id "$execucao" --region "$REGIAO" \
      --query 'JobRun.JobRunState' --output text)

    case "$estado" in
      SUCCEEDED|FAILED|TIMEOUT|STOPPED) break ;;
    esac

    printf '.'
    sleep "$INTERVALO"
    tentativa=$((tentativa + 1))
  done

  echo

  local duracao
  duracao=$(aws glue get-job-run \
    --job-name "$JOB" --run-id "$execucao" --region "$REGIAO" \
    --query 'JobRun.ExecutionTime' --output text)

  if [ "$estado" != "SUCCEEDED" ]; then
    erro "Execucao terminou como ${estado} apos ${duracao}s"
    aws glue get-job-run \
      --job-name "$JOB" --run-id "$execucao" --region "$REGIAO" \
      --query 'JobRun.ErrorMessage' --output text
    erro "Log completo no CloudWatch: /aws-glue/jobs/output"
    exit 1
  fi

  info "Concluida em ${duracao}s"

  # DPU-segundo é a unidade de consumo informada pela propria AWS.
  local dpu
  dpu=$(aws glue get-job-run \
    --job-name "$JOB" --run-id "$execucao" --region "$REGIAO" \
    --query 'JobRun.DPUSeconds' --output text 2>/dev/null || echo "0")

  if [ "$dpu" != "0" ] && [ "$dpu" != "None" ]; then
    info "DPU-segundos consumidos: ${dpu}"
  fi

  RUN_ID="$execucao"
}

verificar_saida() {
  separador
  info "Camada Silver no S3:"
  echo

  aws s3 ls "s3://${BUCKET}/silver/" --recursive --human-readable --summarize \
    --region "$REGIAO" | tail -20
}

main() {
  separador
  info "GLUE JOB — CAMADA SILVER"
  separador

  verificar_pre_requisitos
  executar
  verificar_saida

  separador
  info "Silver gerada. A validacao roda como Job dentro do Workflow."
  info "Log da execucao:"
  echo "    aws logs tail /aws-glue/jobs/output --since 30m --format short"
  separador
}

main "$@"
