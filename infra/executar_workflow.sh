#!/usr/bin/env bash
#
# Dispara o Workflow da camada Silver e acompanha até o fim.
#
# O Workflow encadeia crawler, transformação e qualidade dentro da AWS.
# O Terraform declara o fluxo; iniciar a execução é ação, e por isso fica
# aqui.
#
# Substitui a sequência manual de executar_crawler.sh + executar_job_silver.sh
# + validação. Os scripts individuais continuam disponíveis para depurar
# uma etapa isolada sem rodar o fluxo inteiro.
#
# Uso:
#   bash infra/executar_workflow.sh
#
# Pré-requisito:
#   terraform apply

set -euo pipefail

export MSYS_NO_PATHCONV=1

PREFIXO="${PREFIXO:-alfabetizacao}"
BUCKET="${BUCKET:-fiap-ai-scientist-fase-02}"
REGIAO="${AWS_REGION:-us-east-1}"

WORKFLOW="${PREFIXO}_workflow_silver"

# Relatorios sao gerados pelo Job no S3. Baixar aqui os traz para o
# repositorio, onde servem de evidencia de execucao.
RELATORIOS_LOCAL="${RELATORIOS_LOCAL:-quality/reports}"

INTERVALO=20
MAX_TENTATIVAS=90

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

  if ! aws glue get-workflow --name "$WORKFLOW" --region "$REGIAO" > /dev/null 2>&1; then
    erro "Workflow ${WORKFLOW} nao encontrado."
    erro "Execute antes: cd infra/terraform && terraform apply"
    exit 1
  fi

  info "Workflow ${WORKFLOW} encontrado"
}

disparar() {
  info "Iniciando execucao do fluxo..."

  EXECUCAO=$(aws glue start-workflow-run \
    --name "$WORKFLOW" --region "$REGIAO" \
    --query 'RunId' --output text)

  info "RunId: ${EXECUCAO}"
}

acompanhar() {
  local tentativa=0
  local estado=""
  local anterior=""

  while [ "$tentativa" -lt "$MAX_TENTATIVAS" ]; do
    estado=$(aws glue get-workflow-run \
      --name "$WORKFLOW" --run-id "$EXECUCAO" --region "$REGIAO" \
      --query 'Run.Status' --output text)

    # Progresso por etapa, para nao ficar so pontinhos por cinco minutos
    local resumo
    resumo=$(aws glue get-workflow-run \
      --name "$WORKFLOW" --run-id "$EXECUCAO" --region "$REGIAO" \
      --query 'Run.Statistics.[SucceededActions,FailedActions,RunningActions,TotalActions]' \
      --output text 2>/dev/null || echo "")

    if [ "$resumo" != "$anterior" ] && [ -n "$resumo" ]; then
      echo
      info "Etapas — concluidas/falhas/executando/total: ${resumo//	//}"
      anterior="$resumo"
    fi

    case "$estado" in
      COMPLETED|STOPPED|ERROR) break ;;
    esac

    printf '.'
    sleep "$INTERVALO"
    tentativa=$((tentativa + 1))
  done

  echo

  if [ "$estado" != "COMPLETED" ]; then
    erro "Fluxo terminou como ${estado}"
    detalhar_etapas
    exit 1
  fi

  info "Fluxo concluido"
}

detalhar_etapas() {
  separador
  info "Situacao de cada etapa:"
  echo

  aws glue get-workflow-run \
    --name "$WORKFLOW" --run-id "$EXECUCAO" --region "$REGIAO" \
    --include-graph \
    --query 'Run.Graph.Nodes[].[Type, Name, JobDetails.JobRuns[0].JobRunState, CrawlerDetails.Crawls[0].State]' \
    --output table 2>/dev/null || aviso "Detalhe do grafo indisponivel"
}

verificar_saida() {
  separador
  info "Camada Silver no S3:"
  echo

  aws s3 ls "s3://${BUCKET}/silver/" --recursive --human-readable --summarize \
    --region "$REGIAO" | tail -18

  separador
  info "Relatorio de qualidade:"
  echo

  local relatorio
  relatorio=$(aws s3 ls "s3://${BUCKET}/quality/reports/" --region "$REGIAO" \
    | sort | tail -1 | awk '{print $4}')

  if [ -z "$relatorio" ]; then
    aviso "Nenhum relatorio encontrado"
    return 0
  fi

  # sync e copia pontual, nao sincronizacao continua: traz o que falta e
  # para. Rodar aqui garante que quem executou o fluxo fica com o
  # relatorio em disco, sem precisar de um comando extra.
  mkdir -p "$RELATORIOS_LOCAL"

  aws s3 sync "s3://${BUCKET}/quality/reports/" "$RELATORIOS_LOCAL/" \
    --region "$REGIAO" --only-show-errors

  info "Baixados para ${RELATORIOS_LOCAL}/"
  echo

  cat "${RELATORIOS_LOCAL}/${relatorio}" | head -30
}

main() {
  separador
  info "WORKFLOW — CAMADA SILVER"
  info "crawler da Bronze -> job da Silver -> job de qualidade"
  separador

  verificar_pre_requisitos
  disparar
  acompanhar
  detalhar_etapas
  verificar_saida

  separador
  info "Concluido."
  info "Relatorio completo em ${RELATORIOS_LOCAL}/"
  aviso "Cada execucao gera um arquivo novo. Escolha quais commitar como"
  aviso "evidencia em vez de adicionar a pasta inteira."
  separador
}

main "$@"
