# ==================================================
# FIAP - Tech Challenge Fase 02
# ==================================================

.PHONY: setup install run test-bq test-aws list-buckets freeze clean help

PYTHON = python

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