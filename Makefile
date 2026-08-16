# ==================================================
# FIAP - Tech Challenge Fase 02
# ==================================================

.PHONY: setup install run test-bq test-aws list-buckets freeze clean help \
        check-terraform tf-init tf-plan tf-apply tf-destroy \
        workflow crawler silver silver-completa

PYTHON = python

# Sobrescreva na linha de comando quando necessário:
#     make tf-apply PREFIXO=teste
PREFIXO ?= alfabetizacao
BUCKET ?= fiap-ai-scientist-fase-02

setup:
	$(PYTHON) -m pip install --upgrade pip
	pip install -r requirements.txt

install:
	pip install -r requirements.txt

run:
	$(PYTHON) src/main.py

test-bq:
	$(PYTHON) src/teste_bigquery.py

test-aws:
	aws sts get-caller-identity

list-buckets:
	aws s3 ls

freeze:
	pip freeze > requirements.txt

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Infraestrutura — Terraform ---------------------------------------
#
# Toda a infraestrutura é declarada em infra/terraform: databases,
# crawler, job, upload do script e as tabelas da Silver.
 
# Verifica, não instala: make não é gerenciador de pacotes, e instalar
# software na máquina de quem executa surpreende — além de exigir
# elevação em máquina corporativa.
check-terraform:
	@terraform version > /dev/null 2>&1 && terraform version || \
	  (echo "Terraform nao encontrado."; \
	   echo "  Windows: winget install HashiCorp.Terraform"; \
	   echo "  macOS:   brew install terraform"; \
	   echo "  Linux:   https://terraform.io/downloads"; \
	   exit 1)
 
tf-init: check-terraform
	cd infra/terraform && terraform init -input=false
 
tf-plan: check-terraform
	cd infra/terraform && terraform plan -var="prefixo=$(PREFIXO)"
 
tf-apply: check-terraform
	cd infra/terraform && terraform apply -var="prefixo=$(PREFIXO)"
 
tf-destroy: check-terraform
	cd infra/terraform && terraform destroy -var="prefixo=$(PREFIXO)"
 
# Execução ---------------------------------------------------------
#
# O Terraform declara estado; executar é ação, e fica nos scripts.
 
# Caminho principal: o Workflow encadeia crawler, Silver e qualidade
# dentro da AWS. Uma etapa so dispara se a anterior teve sucesso.
workflow:
	bash infra/executar_workflow.sh
 
# Etapas isoladas, para depurar sem rodar o fluxo inteiro
crawler:
	bash infra/executar_crawler.sh
 
silver:
	bash infra/executar_job_silver.sh
 
# Infraestrutura mais execução orquestrada, do zero ao relatório.
silver-completa: tf-apply workflow
	@echo "Camada Silver concluida e validada"

help:
	@echo ""
	@echo "Comandos disponíveis:"
	@echo " make setup         -> Configura o ambiente"
	@echo " make install       -> Instala dependências"
	@echo " make run           -> Executa pipeline de ingestão"
	@echo " make test-bq       -> Testa conexão com BigQuery"
	@echo " make test-aws      -> Testa autenticação AWS"
	@echo " make list-buckets  -> Lista buckets S3"
	@echo " make freeze        -> Atualiza requirements.txt"
	@echo " make clean         -> Remove cache Python"
	@echo ""
	@echo "Infraestrutura (Terraform):"
	@echo " make tf-init         -> Inicializa o Terraform"
	@echo " make tf-plan         -> Mostra o que seria criado"
	@echo " make tf-apply        -> Cria a infraestrutura"
	@echo " make tf-destroy      -> Remove a infraestrutura"
	@echo ""
	@echo "Camada Silver:"
	@echo " make workflow        -> Executa o fluxo completo na AWS"
	@echo " make silver-completa -> Infra + workflow, do zero ao relatorio"
	@echo ""
	@echo "Etapas isoladas (depuracao):"
	@echo " make crawler         -> So o crawler da Bronze"
	@echo " make silver          -> So o Glue Job da Silver"
	@echo ""