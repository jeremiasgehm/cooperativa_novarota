# Cooperativa Nova Rota

Plataforma analítica e de engenharia de dados concebida sobre o ecossistema **Databricks Lakehouse** e **Unity Catalog**, orientada pelo padrão de **Medallion Architecture (Bronze ➔ Silver ➔ Gold)**. 

O projeto implementa uma esteira incremental orientada a contratos de dados, isolamento de anomalias em quarentena técnica, gestão histórica com **SCD Tipo 2 resiliente a conflitos de merge** e materialização de *data products* analíticos para acompanhamento transacional e financeiro de cooperados.

---

## Arquitetura

O ecossistema opera sob o namespace de três níveis do **Unity Catalog** (`novarota.<schema>.<tabela>`), eliminando caminhos físicos acoplados e centralizando governança, permissões e linhagem técnica:


novarota.bronze.* ──── Raw, append-only, auditoria técnica e schema rescue (_rescued_data)

novarota.silver.* ──── Dados limpos, dimensões históricas e quarentena de desvios

novarota.gold.*   ──── Data Products de cooperados e liquidez por cana

*Consulte a documentação técnica detalhada das decisões em [`docs/architecture.md`](docs/architecture.md) e [`docs/decisions.md`](docs/decisions.md).*

---

## Repositório:

```text
├── .github/workflows/       # Pipelines de CI/CD automatizados
├── docs/                    # Documentação técnica e governança
│   ├── architecture.md      # Topologia, fluxo de dados e governança no Unity Catalog
│   ├── data_contracts.md    # Especificação dos contratos de dados e regras de quarentena
│   └── decisions.md         # Architecture Decision Records (ADRs)
├── notebooks/               # Cadernos de execução no Databricks
│   ├── bronze_ingestion.ipynb
│   ├── silver_processing.ipynb
│   └── gold_data_product.ipynb
├── sql/                     # Consultas analíticas avançadas (Advanced Analytics)
│   └── advanced_analytics.sql
├── src/                     # Módulos Python reutilizáveis e testáveis
│   ├── bronze/              # Helpers de ingestão e metadados
│   ├── silver/              # Regras de qualidade, contratos e SCD Tipo 2
│   └── gold/                # Regras de agregação dos data products
├── tests/                   # Suíte de testes unitários
├── config.py                # Configuração centralizada de catálogos e esquemas
└── README.md
```
---

## Orquestração e Pipeline (Databricks Workflows)

A esteira de processamento é orquestrada de forma sequencial no databricks através do job pipeline_lakehouse_cooperativa_novarota.

Especificação Declarativa do Job:

```YAML
resources:
  jobs:
    pipeline_lakehouse_cooperativa_novarota:
      name: pipeline_lakehouse_cooperativa_novarota
      tasks:
        - task_key: bronze_ingestion
          notebook_task:
            notebook_path: notebooks/bronze_ingestion.ipynb
            source: GIT
          environment_key: Default

        - task_key: silver_processing
          depends_on:
            - task_key: bronze_ingestion
          notebook_task:
            notebook_path: notebooks/silver_processing.ipynb
            source: GIT
          environment_key: Default

        - task_key: gold_data_product
          depends_on:
            - task_key: silver_processing
          notebook_task:
            notebook_path: notebooks/gold_data_product.ipynb
            source: GIT
          environment_key: Default

      git_source:
        git_url: [https://github.com/jeremiasgehm/cooperativa_novarota](https://github.com/jeremiasgehm/cooperativa_novarota)
        git_provider: gitHub
        git_branch: develop

      queue:
        enabled: true

      environments:
        - environment_key: Default
          spec:
            environment_version: "6"

      performance_target: PERFORMANCE_OPTIMIZED
```

---

### Governança no Unity Catalog

Separação entre bronze (imutável), silver (limpeza/conformidade) e gold (consumo).
Rastreabilidade e linhagem ponta a ponta registradas nativamente.

### Ingestão Incremental

- Utilização do formato ```cloudFiles``` para absorção eficiente de arquivos novos via notificações e logs internos de ficheiros.
- Tolerância a alterações de esquema na fonte através da coluna técnica ```_rescued_data```.
- Enriquecimento com metadados de auditoria ```_timestamp_ingestao```, ```_arquivo_origem``` ,```_batch_id```, ```_hash_linha```.

### Contratos de dados e quarentena

- Validação de integridade sem interromper a esteira (non-breaking pipelines): dados que violam contratos não são descartados silenciosamente nem causam falha crítica no job. São roteados para ```novarota.silver.quarentena_*``` com marcação de ```motivo_quarentena``` e ```timestamp``` de auditoria.

### SCD Tipo 2 Desacoplado (Padrão expire + append)

- Prevenção de conflitos ```DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE``` comuns em fontes CDC com múltiplos eventos por chave no mesmo lote.
- Aplicação de deduplicação temporal em janela ```(Window.partitionBy("id_cliente").orderBy(data_atualizacao.desc()))```, seguida de expiração atômica (passo 1: MERGE de update $1 \times 1$) e inserção atômica (passo 2: append seguro apenas quando há mutação de atributos de negócio detectada via hash SHA-256).

### Resolução de chaves na camada gold

- Resolução da linhagem referencial complexa entre transações e cooperados via de-para temporal: ```silver_transacoes ➔ bronze_cartoes_cdc ➔ bronze_contas_cdc ➔ id_cliente```.

---

## Como Executar o Projeto

#### Pré-requisitos

- Workspace Databricks habilitado com Unity Catalog.

- Cluster atribuído ou recurso de computação Serverless ativo.

- Repositório sincronizado via Databricks Git Folders.

#### Passo a Passo de Execução

**1 - Execução Manual (Ambiente de Desenvolvimento):**
- Execute notebooks/bronze_ingestion.ipynb para carregar as tabelas brutas.

- Execute notebooks/silver_processing.ipynb para aplicar contratos de qualidade, quarentena e a atualização SCD Tipo 2 na dimensão de cooperados.

- Execute notebooks/gold_data_product.ipynb para materializar as tabelas analíticas gold_metricas_cooperados e gold_desempenho_canais.

**2 - Execução Automatizada via Workflow (Homologação / Produção):**

- No Databricks, acesse o menu lateral Workflows > Jobs.

- Abra o job pipeline_lakehouse_cooperativa_novarota.

- Clique em Run now para acionar a execução do DAG em lote.

**3 - Extração de Insights Analíticos:**
- Execute as consultas de negócio estruturadas em sql/advanced_analytics.sql no SQL Editor para analisar métricas de ranking de cooperados, taxas de estorno e safras de adesão.