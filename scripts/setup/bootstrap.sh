#!/usr/bin/env bash

set -Eeuo pipefail

##############################################
# FIAP AI Scientist - Tech Challenge Fase 2
# Bootstrap do Projeto
##############################################

GREEN='\033[0;32m'
BLUE='\033[1;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${BLUE}=========================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}=========================================================${NC}"
}

ok() {
    echo -e "${GREEN}✔${NC} $1"
}

create_dir() {
    mkdir -p "$1"

    if [ -z "$(find "$1" -mindepth 1 -print -quit 2>/dev/null)" ]; then
        touch "$1/.gitkeep"
    fi

    ok "$1"
}

create_file() {
    if [ ! -f "$1" ]; then
        touch "$1"
    fi

    ok "$1"
}

##############################################
# Estrutura
##############################################

DIRECTORIES=(

".github/workflows"
".github/ISSUE_TEMPLATE"

"assets/arquitetura"
"assets/diagramas"
"assets/imagens"

"config/cloud"
"config/logging"
"config/pipeline"

"data/bronze/alfabetizacao"
"data/bronze/alunos"
"data/bronze/metas_brasil"
"data/bronze/metas_municipios"
"data/bronze/metas_uf"
"data/bronze/municipios"
"data/bronze/streaming"

"data/silver/dimensoes"
"data/silver/fatos"
"data/silver/integracao"

"data/gold/analytics"
"data/gold/dashboards"
"data/gold/indicadores"
"data/gold/machine_learning"

"data/external"

"docs/api"
"docs/arquitetura"
"docs/finops"
"docs/monitoramento"
"docs/qualidade"

"infra/cloud"
"infra/docker"
"infra/terraform"

"logs"

"monitoring/alerts"
"monitoring/dashboards"
"monitoring/metrics"

"pipelines/batch"
"pipelines/bronze"
"pipelines/gold"
"pipelines/orchestrator"
"pipelines/silver"
"pipelines/streaming"

"quality/expectations"
"quality/reports"
"quality/validations"

"scripts/deploy"
"scripts/etl"
"scripts/setup"
"scripts/utils"

"sql/bronze"
"sql/gold"
"sql/silver"

"src/analytics"
"src/cloud"
"src/config"
"src/finops"
"src/ingestion"
"src/models"
"src/processing"
"src/transformation"
"src/utils"

"tests/e2e"
"tests/integration"
"tests/unit"

)

##############################################
# Arquivos
##############################################

FILES=(

".editorconfig"
".env.example"
".gitignore"
".pre-commit-config.yaml"

"LICENSE"
"README.md"

"Makefile"

"pyproject.toml"

"requirements.txt"
"requirements-dev.txt"

".github/CODEOWNERS"
".github/PULL_REQUEST_TEMPLATE.md"

"data/README.md"
"pipelines/README.md"

"src/__init__.py"
"src/__main__.py"
"src/main.py"

"tests/__init__.py"

)

##############################################
# Execução
##############################################

print_header "CRIANDO DIRETÓRIOS"

for dir in "${DIRECTORIES[@]}"
do
    create_dir "$dir"
done

print_header "CRIANDO ARQUIVOS"

for file in "${FILES[@]}"
do
    create_file "$file"
done

##############################################
# __init__.py
##############################################

print_header "CRIANDO PACOTES PYTHON"

PACKAGES=(

"src/analytics"
"src/cloud"
"src/config"
"src/finops"
"src/ingestion"
"src/models"
"src/processing"
"src/transformation"
"src/utils"

"tests/e2e"
"tests/integration"
"tests/unit"

)

for package in "${PACKAGES[@]}"
do
    create_file "$package/__init__.py"
done

##############################################
# Remove .gitkeep
##############################################

find src -name ".gitkeep" -delete
find tests -name ".gitkeep" -delete

##############################################
# Final
##############################################

print_header "PROJETO CRIADO"

echo -e "${GREEN}"
echo "Estrutura criada com sucesso!"
echo ""
echo "Próximos passos:"
echo ""
echo "git add ."
echo "git commit -m \"feat: cria estrutura inicial do projeto\""
echo ""
echo -e "${NC}"