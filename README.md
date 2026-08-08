# Indicador Criança Alfabetizada — Pipeline Híbrida de Dados

**Tech Challenge · Fase 2 — FIAP AI Scientist**

Pipeline de dados híbrida (batch + streaming) em nuvem, construída sobre Arquitetura Medalhão (Bronze → Silver → Gold), para integrar, tratar e disponibilizar os dados do **Indicador Criança Alfabetizada** — a métrica oficial que acompanha o percentual de estudantes alfabetizados ao final do 2º ano do Ensino Fundamental.

![Status](https://img.shields.io/badge/status-em%20desenvolvimento-yellow)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Cloud](https://img.shields.io/badge/cloud-AWS-orange)
![Fonte](https://img.shields.io/badge/fonte-BigQuery-4285F4)
![License](https://img.shields.io/badge/license-MIT-green)

> **Legenda de status:** ✅ concluído · 🚧 em andamento · ⏳ planejado
> **Documentação aprofundada:** este README conta a história completa do projeto. Os detalhes técnicos de cada área estão em [`/docs`](docs/) — veja o [índice da documentação](docs/README.md).

---

## Sumário

1. [O problema](#1-o-problema)
2. [A solução em uma página](#2-a-solução-em-uma-página)
3. [Fonte de dados](#3-fonte-de-dados)
4. [Arquitetura](#4-arquitetura)
5. [Decisões arquiteturais e trade-offs](#5-decisões-arquiteturais-e-trade-offs)
6. [As camadas do data lake](#6-as-camadas-do-data-lake)
7. [Qualidade de dados](#7-qualidade-de-dados)
8. [Ingestão em streaming](#8-ingestão-em-streaming)
9. [Observabilidade e monitoramento](#9-observabilidade-e-monitoramento)
10. [FinOps — custo e otimização](#10-finops--custo-e-otimização)
11. [Aplicação em IA e políticas públicas](#11-aplicação-em-ia-e-políticas-públicas)
12. [Como executar](#12-como-executar)
13. [Evidências de execução](#13-evidências-de-execução)
14. [Estrutura do repositório](#14-estrutura-do-repositório)
15. [Fluxo de trabalho Git](#15-fluxo-de-trabalho-git)
16. [Roadmap e status](#16-roadmap-e-status)
17. [Documentação complementar](#17-documentação-complementar)
18. [Equipe](#18-equipe)
19. [Licença](#19-licença)

---

## 1. O problema

O Brasil assumiu o compromisso de alfabetizar **todas as crianças até o final do 2º ano do Ensino Fundamental até 2030**. Para medir o avanço, o INEP aplica uma avaliação padronizada e considera alfabetizado o estudante que atinge **743 pontos na escala Saeb de Língua Portuguesa**.

O desafio de dados não está em coletar a informação — ela é pública. Está em **integrá-la e mantê-la confiável ao longo do tempo**. Os resultados por aluno, as metas por ente federativo e as dimensões territoriais são tabelas distintas, com granularidades distintas, produzidas em momentos distintos. Cruzá-las exige normalizar códigos territoriais, conciliar representações diferentes da mesma rede de ensino e decidir o que fazer quando duas fontes discordam sobre o mesmo número.

Sem uma camada integrada, cada análise vira retrabalho manual: alguém consulta a base, monta um cruzamento em planilha, chega a um número — e ninguém consegue reproduzir aquele número três meses depois. **Gestores educacionais não conseguem responder perguntas simples de forma confiável e recorrente:** quais municípios estão abaixo da meta, onde o avanço estagnou, quais redes melhoraram e quanto.

Esta pipeline resolve esse gargalo. Transforma dados públicos brutos em uma camada analítica versionada, validada e reprodutível, pronta para consumo por dashboards, análises estatísticas e modelos de IA.

---

## 2. A solução em uma página

| Dimensão | O que entregamos |
|---|---|
| **Ingestão** | Extração programática das tabelas públicas via BigQuery (batch) + produtor de eventos simulando atualizações do indicador (streaming) |
| **Armazenamento** | Data lake em Amazon S3 em Arquitetura Medalhão, com dados em Parquet particionado |
| **Tratamento** | Padronização de esquemas, normalização de chaves territoriais, tipagem correta e integração das entidades |
| **Qualidade** | Validações automatizadas com relatório versionado a cada execução — registro reprovado vai para quarentena, não é descartado em silêncio |
| **Camada Gold** | Datasets analíticos prontos: indicador por município, meta × resultado, série temporal e tabela de features para ML |
| **Operação** | Logging estruturado, métricas de execução e alertas de falha |
| **FinOps** | Arquitetura serverless, formato colunar particionado e estimativa de custo mensal documentada |

**O diferencial da entrega é a reprodutibilidade.** Qualquer avaliador clona o repositório, configura as credenciais e reconstrói a pipeline inteira do zero com um comando. Nenhuma etapa depende de download manual, planilha local ou arquivo que alguém precisa mandar por e-mail.

---

## 3. Fonte de dados

Todas as entidades vêm de uma única fonte: o dataset público **`basedosdados.br_inep_avaliacao_alfabetizacao`**, hospedado no BigQuery pela plataforma [Base dos Dados](https://basedosdados.org/).

O INEP é o **produtor** do dado; a Base dos Dados é o **meio de acesso** — ela publica os microdados oficiais já tratados, tipados e consultáveis via SQL. A extração acontece em `src/ingestion/extract.py`.

| Tabela | Conteúdo | Grão | Destino na Bronze |
|---|---|---|---|
| `uf` | Indicador agregado por unidade federativa | UF × ano | `data/bronze/alfabetizacao/` |
| `municipio` | Dimensão territorial de municípios | Município | `data/bronze/municipios/` |
| `alunos` | Resultados no nível do estudante | Aluno | `data/bronze/alunos/` |
| `meta_alfabetizacao_brasil` | Meta nacional | País × ano | `data/bronze/metas_brasil/` |
| `meta_alfabetizacao_uf` | Meta por unidade federativa | UF × ano | `data/bronze/metas_uf/` |
| `meta_alfabetizacao_municipio` | Meta por município | Município × ano | `data/bronze/metas_municipios/` |
| `dicionario` | Dicionário de dados das tabelas | Coluna | `data/bronze/dicionario/` |

**O código IBGE de município (7 dígitos) é a espinha dorsal da integração.** Todas as entidades territoriais convergem nele, e por isso ele é tratado como *string* em toda a pipeline — preservar zeros à esquerda e a semântica de código, não de número, é pré-requisito para o join funcionar. A regra precisa valer para todas as chaves, sem exceção silenciosa.

**Por que uma fonte única e não várias.** Chegamos a explorar os microdados brutos do INEP em CSV e as planilhas oficiais de metas em XLSX. Descartamos esse caminho: ele agrega variabilidade de formato — duas linhas de cabeçalho, nulos codificados como texto, safras com precisão divergente — sem agregar informação que a Base dos Dados já não entregue tratada. Trocamos superfície de erro por consistência. A decisão e as alternativas descartadas estão em **[ADR-001](docs/arquitetura/adr-001-fonte-de-dados.md)**.

📄 *Aprofundamento:* [esquema completo das tabelas e mapa de chaves](docs/qualidade/) ⏳

---

## 4. Arquitetura

```mermaid
flowchart LR
    subgraph FONTE["Fonte pública"]
        BQ[("Base dos Dados · BigQuery<br/>br_inep_avaliacao_alfabetizacao")]
    end

    subgraph INGEST["Ingestão · src/ingestion"]
        BATCH["Extração batch<br/>Python + SQL"]
        STREAM["Produtor de eventos<br/>streaming"]
    end

    subgraph LAKE["Data Lake — Amazon S3"]
        BRONZE["🥉 Bronze<br/>Parquet fiel à origem"]
        SILVER["🥈 Silver<br/>limpo, padronizado<br/>e integrado"]
        GOLD["🥇 Gold<br/>datasets analíticos"]
    end

    subgraph CONSUMO["Consumo"]
        DASH["Dashboard"]
        ML["Modelos de IA"]
        SQL["Athena<br/>consultas ad hoc"]
    end

    QA{{"Validações<br/>de qualidade"}}
    OBS[["Logging, métricas<br/>e alertas"]]

    BQ --> BATCH
    BQ -.simula atualizações.-> STREAM
    BATCH --> BRONZE
    STREAM --> BRONZE
    BRONZE --> QA --> SILVER
    SILVER --> QA
    SILVER --> GOLD
    GOLD --> DASH
    GOLD --> ML
    GOLD --> SQL
    OBS -.observa.-> INGEST
    OBS -.observa.-> LAKE
```

> ⏳ Complementar com diagrama em `assets/arquitetura/` usando os ícones oficiais AWS.

**Princípio que orienta todo o projeto: o repositório contém apenas código; os dados moram no S3.** O `.gitignore` bloqueia arquivos de dados em `data/`, que existe localmente só como área de trabalho. Versionar Parquet de milhões de linhas estouraria os limites do GitHub e contaminaria o histórico — que é, ele próprio, critério de avaliação.

### Stack

| Camada | Tecnologia | Papel | Status |
|---|---|---|---|
| Linguagem | Python 3.11 | Toda a pipeline | ✅ |
| Extração | `google-cloud-bigquery` 3.42 | Consulta à fonte | ✅ |
| Manipulação | `pandas` 3.0 · `pyarrow` 25.0 | Transformações e escrita Parquet | ✅ |
| Configuração | `python-dotenv` + dataclass | Centralizada em `src/config/settings.py` | ✅ |
| Configuração | `pydantic-settings` | Gerenciamento tipado das configurações | ✅ |
| Build | `Makefile` | Automação das principais tarefas do projeto | ✅ |
| Armazenamento | Amazon S3 (`boto3` 1.43) | Data lake medalhão com upload automático da camada Bronze | ✅ |
| Formato | Apache Parquet + Snappy | Colunar comprimido | ✅ |
| Consulta | Amazon Athena | SQL sobre o lake | ⏳ |
| Streaming | ⏳ *a definir* | Ingestão near real-time simulada | ⏳ |
| Orquestração | Makefile | Encadeamento das etapas | ⏳ |
| Qualidade | ⏳ *a definir* | Regras de validação | ⏳ |
| Dashboard | ⏳ *a definir* | Visualização analítica | ⏳ |
| Padronização | `ruff` · `black` · `pytest` · `pre-commit` | Qualidade de código | ⏳ |

📄 *Aprofundamento:* [diagrama detalhado, modelo dimensional e fluxo de dados](docs/arquitetura/) ⏳

---

## 5. Decisões arquiteturais e trade-offs

Cada decisão relevante é registrada como ADR (*Architecture Decision Record*) em [`docs/arquitetura/`](docs/arquitetura/), no formato contexto → decisão → alternativas → trade-off. O resumo está aqui; a análise completa, no documento correspondente.

### ADR-001 · Base dos Dados via BigQuery como fonte única

Consumimos as tabelas do dataset público no BigQuery, em vez de baixar microdados CSV e planilhas de metas direto do INEP.

**Trade-off.** A Base dos Dados adiciona um intermediário entre nós e o dado oficial — se ela atrasar uma atualização, atrasamos junto. Em troca, recebemos tipagem consistente, esquema estável entre edições e acesso por SQL. O caminho direto ao INEP daria independência, ao custo de absorver toda a variabilidade de formato das planilhas. Para um projeto cuja complexidade real está na *integração* e não na *aquisição*, investir esforço em parsing de XLSX seria otimizar o lugar errado.

📄 [Leia a ADR-001 completa](docs/arquitetura/adr-001-fonte-de-dados.md)

### ADR-002 · BigQuery como fonte, AWS como plataforma

O GCP entra exclusivamente como ponto de extração. Todo o armazenamento e processamento acontece na AWS.

**Trade-off.** Manter tudo no GCP eliminaria uma nuvem da equação e simplificaria a gestão de credenciais. Optamos pelo S3 porque o data lake é o centro da arquitetura pedida, e porque a situação "a fonte mora no ecossistema X, o processamento no Y" é corriqueira em ambientes reais — resolvê-la é parte do exercício, não um desvio dele. O custo é gerenciar dois conjuntos de credenciais.

📄 [Leia a ADR-002 completa](docs/arquitetura/) ⏳

### ADR-003 · Parquet já na camada Bronze

A ingestão grava Parquet diretamente, sem CSV intermediário.

**Trade-off.** A definição canônica de Bronze é "dado bruto, sem transformação significativa", e converter o formato tecnicamente tensiona isso. Aceitamos porque a conversão é *lossless* — nenhum valor se altera, apenas a serialização — e o ganho em custo de armazenamento e velocidade de leitura é imediato. O que não seria aceitável é a conversão acontecer sem estar documentada.

📄 [Leia a ADR-003 completa](docs/arquitetura/) ⏳

### ADR-004 · Serverless em vez de cluster gerenciado

S3 + Athena + execução sob demanda, sem cluster Spark permanente.

**Trade-off.** Um EMR ou Glue escalaria melhor para volumes ordens de grandeza maiores, mas cobra por cluster ativo mesmo ocioso. O volume aqui é grande para planilha e pequeno para Big Data — dimensionar a arquitetura ao problema real, e não à moda arquitetural, mantém a conta próxima de zero e o tempo de execução em minutos. O limite é conhecido: se o escopo crescesse dez vezes, a decisão precisaria ser revisitada.

📄 [Leia a ADR-004 completa](docs/arquitetura/) ⏳

### ADR-005 · Códigos territoriais como *string*

`id_municipio`, `id_uf` e demais identificadores são texto em todas as camadas.

**Trade-off.** Ocupa mais espaço que inteiro e exige atenção nos joins. Em troca, elimina uma classe inteira de falha silenciosa: código que perde zero à esquerda não lança erro, apenas deixa de casar no join — e o município desaparece do resultado sem nenhum aviso.

📄 [Leia a ADR-005 completa](docs/arquitetura/) ⏳

---

## 6. As camadas do data lake

### 🥉 Bronze — fidelidade à origem

Recebe as tabelas como vieram da consulta, sem interpretação, uma pasta por entidade. Cada ingestão registra metadados de proveniência: data/hora, tabela de origem, volume extraído e checksum.

**Regra:** a Bronze é imutável e append-only. Inconsistência que existe na origem permanece na Bronze — corrigir é trabalho da Silver. Isso preserva a capacidade de auditar o que a fonte realmente entregou em cada data.

### 🥈 Silver — limpa, padronizada e integrada

É onde mora a complexidade real do projeto:

- **Normalização de chaves territoriais** — o join que sustenta todo o resto.
- **Tipagem correta**, com códigos como string (ADR-005).
- **Tratamento de nulos e ausências**, distinguindo "não informado" de "zero".
- **Conciliação entre representações** da mesma rede de ensino nas diferentes tabelas.
- **Integração das entidades** em um modelo dimensional de fatos e dimensões.
- **Deduplicação** e aplicação das regras de qualidade.

📄 *Aprofundamento:* [modelo dimensional e mapeamento coluna a coluna](docs/arquitetura/) ⏳

### 🥇 Gold — pronta para consumo

Datasets desnormalizados, agregados no grão de análise:

| Dataset | Grão | Uso |
|---|---|---|
| `indicador_municipio` | Município × ano × rede | Indicador com flag de cobertura |
| `meta_vs_resultado` | Município / UF / Brasil × ano | Distância até a meta |
| `evolucao_temporal` | Município × ano | Série histórica em painel balanceado |
| `features_ml` | Município × ano | Tabela de features para modelagem |

---

## 7. Qualidade de dados

Qualidade é entregável de primeira classe, não verificação final. Cada regra corresponde a um risco concreto identificado nas tabelas — nenhuma é checagem genérica de manual.

| # | Regra | Ação quando falha |
|---|---|---|
| Q1 | Integridade referencial de `id_municipio` entre indicador, metas e dimensão territorial | Bloqueia promoção para Gold |
| Q2 | Códigos territoriais como string, com 7 dígitos preservados | Falha de esquema |
| Q3 | Unicidade da chave natural em cada tabela | Log + quarentena |
| Q4 | Registros sem vínculo territorial identificável | **Quarentena**, nunca descarte silencioso |
| Q5 | Coerência entre indicador de alfabetização e proficiência ≥ 743 | Teste de integridade contínuo |
| Q6 | Cobertura por ano e UF dentro do esperado | Alerta de variação anômala |
| Q7 | Nulos distinguidos de zeros em campos numéricos | Correção documentada |
| Q8 | Volume de registros por camada dentro da faixa histórica | Alerta de perda silenciosa |

**Princípio de quarentena.** Registro reprovado não some — vai para área isolada com o motivo da reprovação registrado. Descarte silencioso corrompe indicadores sem deixar rastro, e em dado educacional isso significa um município simplesmente desaparecendo da análise. O gestor daquele município nunca saberia que foi excluído.

Cada execução gera relatório versionado em `quality/reports/`, permitindo comparar a qualidade ao longo do tempo — porque a fonte muda, e a pipeline precisa perceber quando isso acontece.

📄 *Aprofundamento:* [catálogo completo de regras, implementação e relatórios](docs/qualidade/) ⏳

---

## 8. Ingestão em streaming

Os dados de alfabetização são **anuais por natureza**: não existe fluxo real em tempo quase real. A camada de streaming simula um cenário plausível de operação contínua — atualizações incrementais chegando fora do ciclo batch: retificações do INEP, correções municipais, novas divulgações.

O produtor publica eventos em um tópico; o consumidor grava na Bronze em partição própria (`data/bronze/streaming/`), preservando a distinção entre o que veio do ciclo batch e o que chegou por evento.

**Por que isso não é enfeite acadêmico.** Retificação na fonte oficial acontece de fato — o INEP já removeu registros inconsistentes de edições passadas depois da publicação. Uma arquitetura que só sabe reprocessar o lote inteiro trata correção pontual como evento caro e raro, e na prática as pessoas param de aplicar correções. Modelar a chegada incremental desde o início é decisão de desenho.

📄 *Aprofundamento:* [desenho do produtor, formato do evento e consumo](docs/arquitetura/) ⏳

---

## 9. Observabilidade e monitoramento

| O que monitoramos | Como | Por quê |
|---|---|---|
| Sucesso/falha de cada etapa | Logging estruturado em JSON | Rastrear onde a pipeline quebrou |
| Volume processado por camada | Contagem de registros entrada/saída | Detectar perda silenciosa |
| Latência de execução | Tempo por etapa | Identificar degradação e gargalos |
| Taxa de reprovação em qualidade | Métrica por regra | Perceber mudança na fonte |
| Custo acumulado | Métrica FinOps | Evitar surpresa na fatura |

**O alerta mais importante não é o de falha — é o de sucesso anômalo.** Uma pipeline que roda sem erro mas processa metade dos registros da execução anterior é mais perigosa do que uma que quebra, porque entrega um dashboard com números plausíveis e errados. Ninguém investiga o que não deu erro. Por isso a contagem de registros por camada é métrica de primeira ordem, com faixa esperada e alerta próprio.

**Otimização.** Os tempos por etapa alimentam decisões concretas de tuning: estratégia de particionamento, colunas efetivamente lidas nas consultas Athena e paralelização das extrações independentes entre si.

📄 *Aprofundamento:* [runbook operacional, catálogo de alertas e métricas](docs/monitoramento/) ⏳

---

## 10. FinOps — custo e otimização

| Decisão | Efeito |
|---|---|
| Parquet + compressão Snappy | Reduz drasticamente o volume frente a CSV |
| Particionamento por ano e UF | Athena varre só a partição necessária — cobrança é por dado escaneado |
| Formato colunar | Consultas leem apenas as colunas usadas |
| Arquitetura serverless | Nenhum recurso ocioso sendo cobrado |
| Lifecycle policy no S3 | Bronze antiga migra para classe mais barata |
| Seleção explícita de colunas na extração | Reduz o volume escaneado no BigQuery |

### Estimativa mensal

> 🚧 *Preencher com valores reais após a implantação. A estimativa é pedida explicitamente no enunciado.*

| Serviço | Uso estimado | Custo |
|---|---|---|
| Amazon S3 — armazenamento | ⏳ GB | ⏳ US$ |
| Amazon S3 — requisições | ⏳ | ⏳ US$ |
| Amazon Athena | ⏳ GB escaneados | ⏳ US$ |
| BigQuery — extração | ⏳ TB processados | ⏳ US$ |
| Streaming | ⏳ | ⏳ US$ |
| **Total** | | **⏳ US$/mês** |

**Atenção ao BigQuery:** a cobrança é por volume escaneado na consulta, não por linha retornada. Um `SELECT *` sem filtro varre a tabela inteira mesmo com `LIMIT` — o limite corta o retorno, não a varredura. Selecionar apenas as colunas necessárias é a otimização de maior impacto na etapa de extração.

📄 *Aprofundamento:* [memória de cálculo, premissas e simulação de cenários](docs/finops/) ⏳

---

## 11. Aplicação em IA e políticas públicas

A camada Gold não é o fim da pipeline — é o insumo da próxima etapa. Foi desenhada desde o início pensando em consumo por modelos.

### Casos de uso em IA

**Predição do indicador municipal.** Alvo contínuo (percentual de alunos alfabetizados) no grão município × ano, com poucos milhares de observações anuais — volume adequado para modelos tabulares. Enriquecível com Censo Escolar, IBGE/PNAD e indicadores socioeconômicos.

**Classificação de risco de não atingimento de meta.** Alvo binário derivado de meta × resultado. Entrega ao gestor uma lista priorizada de municípios em risco *antes* do fim do ciclo, transformando um indicador retrospectivo em sinal de alerta acionável.

**Clusterização de vulnerabilidade educacional.** Agrupamento não supervisionado de municípios por perfil. Permite desenhar intervenções por tipologia, em vez de tratar mais de cinco mil municípios como casos individuais ou, pior, como média nacional.

**Análise além da média.** A distribuição por níveis de desempenho permite identificar a massa de alunos imediatamente abaixo do ponto de corte — o grupo em que intervenção pedagógica focalizada tem maior retorno marginal. A média esconde exatamente esse grupo.

### Impacto em políticas públicas

| Pergunta do gestor | Como a Gold responde |
|---|---|
| Quais municípios estão mais distantes da meta? | Ranking de gap por meta × resultado |
| Onde o avanço estagnou? | Série temporal com variação ano a ano |
| A desigualdade entre regiões está aumentando? | Dispersão do indicador por UF ao longo do tempo |
| Onde alocar recurso adicional? | Cruzamento de risco predito com tamanho da rede |

**Ressalva de uso responsável.** O indicador mede um recorte específico da alfabetização em um momento específico. Usá-lo para ranquear e punir redes cria incentivo para otimizar o número, não o aprendizado — e o número é sempre mais fácil de otimizar. A camada Gold é desenhada para diagnóstico e alocação de recurso. Essa é uma decisão de projeto, não uma observação acessória.

---

## 12. Como executar

### Pré-requisitos

- Python 3.11
- Projeto no GCP com a API do BigQuery habilitada e credenciais de service account
- Conta AWS com acesso ao bucket S3 do projeto

### Instalação

```bash
git clone https://github.com/luizafcunha/fiap-ai-scientist-fase-02.git
cd fiap-ai-scientist-fase-02

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -r requirements.txt

cp .env.example .env             # preencher com os valores do seu ambiente
```

### Variáveis de ambiente

| Variável | Descrição |
|---|---|
| `GCP_PROJECT_ID` | Projeto GCP usado para faturar a consulta ao BigQuery |
| `GCP_DATASET` | Dataset consultado |
| `GOOGLE_APPLICATION_CREDENTIALS` | Caminho do JSON da service account |
| `AWS_REGION` | Região do bucket |
| `AWS_BUCKET` | Bucket do data lake |
| `PIPELINE_ENV` | `dev` ou `prod` |

> ⚠️ **Nenhuma credencial vai para o Git.** O `.env` está no `.gitignore`; o `.env.example` traz apenas os nomes das variáveis.


### Configuração da AWS

```bash
aws configure
```

Em ambientes AWS Academy configure também:

```bash
aws configure set aws_session_token <TOKEN>
```

Validação:

```bash
aws sts get-caller-identity
aws s3 ls
```

### Configuração do Google Cloud

```text
GOOGLE_APPLICATION_CREDENTIALS=/caminho/credenciais.json
```

Validação:

```bash
python src/teste_bigquery.py
```


### Execução

**Estado atual** ✅ — extração e escrita da Bronze:

```bash
python src/main.py
```

Extrai as tabelas do BigQuery, grava arquivos Parquet na camada Bronze local e realiza automaticamente o upload para o bucket Amazon S3. O comando deve ser executado a partir da raiz do repositório.

**Alvos planejados** ⏳ — via Makefile:

```bash
make bronze      # extrai a fonte e grava na Bronze
make s3          # sincroniza a Bronze com o bucket
make silver      # trata, padroniza e integra
make gold        # gera os datasets analíticos
make quality     # roda as validações e emite o relatório
make streaming   # inicia o produtor de eventos
make test        # executa a suíte de testes
make pipeline    # executa tudo em sequência
```

### Execução via Makefile

```bash
make help
make install
make run
make test-bq
make clean
```

📄 *Aprofundamento:* [referência dos módulos](docs/api/) ⏳

---

## 13. Evidências de execução

> ⏳ *Seção a preencher — comprova que a pipeline efetivamente rodou.*

| Evidência | Link |
|---|---|
| Vídeo — pipeline executando ponta a ponta | ⏳ |
| Vídeo executivo (até 5 min) | ⏳ |
| Print — estrutura medalhão no bucket S3 | `assets/imagens/` ⏳ |
| Print — log completo de execução | `assets/imagens/` ⏳ |
| Print — relatório de qualidade | `assets/imagens/` ⏳ |
| Print — dashboard analítico | `assets/imagens/` ⏳ |
| Print — consulta Athena sobre a Gold | `assets/imagens/` ⏳ |
| Relatório de execução com métricas | `docs/monitoramento/` ⏳ |

---

## 14. Estrutura do repositório

```
.
├── assets/          # diagramas, imagens e evidências visuais
├── config/          # configurações de cloud, logging e pipeline
├── data/            # área local das camadas (dados NÃO versionados)
├── docs/            # documentação aprofundada — ver docs/README.md
├── infra/           # infraestrutura como código
├── logs/            # logs de execução (não versionados)
├── monitoring/      # alertas, dashboards e métricas
├── pipelines/       # orquestração por camada e por modo
├── quality/         # expectativas, validações e relatórios
├── scripts/         # bootstrap, ETL e utilitários
├── sql/             # consultas por camada
├── src/             # código-fonte
│   ├── config/          #   settings centralizado
│   ├── ingestion/       #   extração (BigQuery) e escrita (Parquet)
│   ├── transformation/  #   Bronze → Silver
│   ├── processing/      #   Silver → Gold
│   ├── analytics/       #   agregações analíticas
│   ├── cloud/           #   integração com S3
│   ├── finops/          #   monitoramento de custos
│   ├── models/          #   modelos de dados
│   └── utils/           #   utilitários compartilhados
└── tests/           # testes unitários, de integração e e2e
```

---

## 15. Fluxo de trabalho Git

O histórico do repositório é parte da entrega. Nada é commitado direto na `main`.

### Branches

| # | Branch | Objetivo | Tipo | Status |
|---|---|---|---|---|
| 1 | `feature/estrutura-inicial` | Estrutura inicial do projeto | `feat` | ✅ |
| 2 | `feature/configuracao-ambiente` | Configuração do ambiente | `chore` | ✅ |
| 3 | `feature/configuracao-aplicacao` | Centralização das configurações | `chore` | ✅ |
| 4 | `feature/extracao-bigquery` | Extração de dados do BigQuery | `feat` | 🚧 |
| 5 | `feature/camada-bronze` | Implementação da camada Bronze | `feat` | 🚧 |
| 6 | `feature/upload-s3` | Upload da Bronze para o Amazon S3 | `feat` | ✅ |
| 7 | `feature/camada-silver` | Implementação da camada Silver | `feat` | ⏳ |
| 8 | `feature/camada-gold` | Implementação da camada Gold | `feat` | ⏳ |
| 9 | `feature/qualidade-dados` | Validações de qualidade | `feat` | ⏳ |
| 10 | `feature/logging-monitoramento` | Logging e monitoramento | `feat` | ⏳ |
| 11 | `feature/streaming` | Ingestão em streaming | `feat` | ⏳ |
| 12 | `feature/finops` | Monitoramento de custos | `feat` | ⏳ |
| 13 | `feature/dashboard` | Dashboard analítico | `feat` | ⏳ |
| 14 | `feature/documentacao` | Documentação técnica e operacional | `docs` | 🚧 |
| 15 | `feature/ci-cd` *(opcional)* | Integração e entrega contínua | `chore` | ⏳ |

### Padrão de commits

Seguimos [Conventional Commits](https://www.conventionalcommits.org/pt-br/):

```
<tipo>: <descrição no imperativo, em minúsculas>

feat: implementa extração das tabelas de metas via BigQuery
fix: corrige perda de zeros à esquerda no código de município
docs: registra ADR-004 sobre arquitetura serverless
chore: configura pre-commit com ruff e black
test: adiciona teste de integridade referencial da Silver
```

### Pull Requests

Toda branch entra na `main` por PR, usando o template em [`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md). A descrição explica, em linguagem simples:

- **O que** foi feito
- **Por que** foi feito assim — decisões tomadas e alternativas descartadas
- **Como validar** — comando para reproduzir e evidência de execução
- **O que ficou de fora** e por quê

> O PR é o registro da evolução do projeto. Descrição vaga hoje é contexto perdido amanhã — e é o histórico que demonstra como o trabalho foi construído, não apenas o resultado final.

---

## 16. Roadmap e status

| Etapa | Entregável | Status |
|---|---|---|
| Fundação | Estrutura do repositório e fluxo Git | ✅ |
| Fundação | Configuração de ambiente e aplicação | ✅ |
| Fundação | Exploração das fontes | ✅ |
| Fundação | ADRs e diagrama de arquitetura | 🚧 |
| Bronze | Extração via BigQuery | 🚧 |
| Bronze | Escrita em Parquet | ✅ |
| Bronze | Upload para o S3 | ✅ |
| Bronze | Produtor de eventos (streaming) | ⏳ |
| Silver | Transformações e padronização | ⏳ |
| Silver | Integração das entidades | ⏳ |
| Silver | Validações de qualidade | ⏳ |
| Gold | Datasets analíticos | ⏳ |
| Operação | Logging e monitoramento | ⏳ |
| Operação | FinOps e estimativa de custo | ⏳ |
| Consumo | Dashboard analítico | ⏳ |
| Entrega | README e documentação | 🚧 |
| Entrega | Evidências de execução | ⏳ |
| Entrega | Vídeo executivo | ⏳ |

---

## 17. Documentação complementar

Este README é autossuficiente: quem ler só ele entende o problema, a arquitetura, as decisões e como executar. Os documentos abaixo aprofundam cada área para quem quiser o detalhe técnico.

| Área | Conteúdo | Local |
|---|---|---|
| **Índice geral** | Mapa de toda a documentação | [`docs/README.md`](docs/README.md) |
| **Arquitetura** | ADRs, diagramas e modelo dimensional | [`docs/arquitetura/`](docs/arquitetura/) |
| **Qualidade** | Catálogo de regras, dicionário de dados, relatórios | [`docs/qualidade/`](docs/qualidade/) |
| **Monitoramento** | Runbook, alertas e métricas | [`docs/monitoramento/`](docs/monitoramento/) |
| **FinOps** | Memória de cálculo e cenários de custo | [`docs/finops/`](docs/finops/) |
| **API** | Referência dos módulos de `src/` | [`docs/api/`](docs/api/) |

---

## 18. Equipe

| Nome        | Responsabilidade principal | GitHub |
|-------------|----------------------------|--------|
| Amanda      | ⏳                         | [@Amanda](https://github.com/Amanda) |
| Antoni Lima | ⏳                         | [@AntoniLima](https://github.com/AntoniLima) |
| Joviniano   | ⏳                         | [@Joviniano](https://github.com/Joviniano) |
| Luiza Cunha | ⏳                         | [@luizafcunha](https://github.com/luizafcunha) |
| Vinicius    | ⏳                         | [@Vinicius](https://github.com/Vinicius) |

**Curso:** Pós-graduação FIAP — AI Scientist · **Fase:** 2 — Engenharia de Dados · **Ano:** 2026

---

## 19. Licença

Distribuído sob a licença MIT. Veja [LICENSE](LICENSE).

Os dados utilizados são públicos, produzidos pelo INEP e disponibilizados pela plataforma Base dos Dados, sujeitos aos termos de uso originais de cada fonte.
