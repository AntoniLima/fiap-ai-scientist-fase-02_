output "databases" {
  description = "Databases criados no Glue Catalog."
  value = {
    for chave, db in aws_glue_catalog_database.camada :
    chave => db.name
  }
}

output "localizacoes" {
  description = "Caminho no S3 associado a cada database."
  value = {
    for chave, db in aws_glue_catalog_database.camada :
    db.name => db.location_uri
  }
}

output "total" {
  description = "Quantidade de databases gerenciados."
  value       = length(aws_glue_catalog_database.camada)
}

output "crawler_bronze" {
  description = "Crawler que cataloga a camada Bronze."
  value       = aws_glue_crawler.bronze.name
}

output "comando_executar_crawler" {
  description = "O Terraform cria o crawler, mas nao o executa."
  value       = "aws glue start-crawler --name ${aws_glue_crawler.bronze.name}"
}

output "job_silver" {
  description = "Glue Job da camada Silver."
  value       = aws_glue_job.silver.name
}

output "comando_executar_job" {
  description = "O Terraform cria o Job, mas nao o executa — executar e acao, nao estado."
  value       = "aws glue start-job-run --job-name ${aws_glue_job.silver.name}"
}

output "tabelas_silver" {
  description = "Tabelas da camada Silver declaradas no Catalog."
  value       = sort([for t in aws_glue_catalog_table.silver : t.name])
}

output "workflow" {
  description = "Workflow que orquestra crawler, Silver e qualidade."
  value       = aws_glue_workflow.silver.name
}

output "comando_executar_workflow" {
  description = "O Terraform declara o fluxo; iniciar a execucao e acao."
  value       = "aws glue start-workflow-run --name ${aws_glue_workflow.silver.name}"
}
