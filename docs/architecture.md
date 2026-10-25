# Arquitetura Tecnica e Governanca

Este documento apresenta o desenho tecnico da plataforma Lakehouse da Cooperativa Nova Rota, detalhando a topologia de dados no Unity Catalog, o fluxo de linhagem ponta a ponta, os mecanismos de resiliencia, o modelo de governanca e a modularizacao do codigo.

!["Arquitetura"](/Workspace/Users/jeremiasgehm@gmail.com/cooperativa_novarota/docs/evidences/arquitetura.png)

---

## 1- Topologia de Dados e Governanca no Unity Catalog

A plataforma foi implementada utilizando o namespace formal de tres niveis do Databricks Unity Catalog (`novarota.<camada>.<tabela>`). Essa abordagem desacopla o codigo de caminhos fisicos de armazenamento (*storage locations*) e centraliza o controle de acessos (RBAC), auditoria e linhagem tecnica:

```text
                           novarota
                          (Catalogo)
        ┌──────────────────────┼──────────────────────┐
        ▼                      ▼                      ▼
     bronze                  silver                 gold
(Raw / Ingestao)        (Limpeza / SCD2)       (Data Products)
```

### Camada Bronze (`novarota.bronze`)

- **Objetivo:** Ingestao bruta, append-only e imutavel das origens transacionais e fluxos de Change Data Capture (CDC).
- **Mecanismo:** Databricks Auto Loader (`format("cloudFiles")`), operando por notificacoes de eventos e logs de arquivos no storage para ingestao incremental de baixa latencia.
- **Schema Evolution & Rescue:** Inclusao automatica da coluna tecnica `_rescued_data`, prevenindo falhas de pipeline diante de colunas nao mapeadas ou dados corrompidos.
- **Metadados Tecnicos de Auditoria Forense:**
    - `_timestamp_ingestao`: Timestamp UTC preciso da execucao da micro-tarefa.
    - `_arquivo_origem`: Caminho completo do arquivo de origem no storage.
    - `_batch_id`: Identificador de execucao do lote para controle de idempotencia.
    - `_hash_linha`: Hash criptografico SHA-256 do registro bruto para verificacao de integridade e rastreabilidade de duplicidades.
- **Tabelas Geradas:**
    - `bronze_clientes_cdc`: Historico bruto de mutacoes cadastrais de cooperados.
    - `bronze_contas_cdc`: Historico bruto de abertura e estado operacional das contas.
    - `bronze_cartoes_cdc`: Cadastro e atualizacao de cartoes vinculados.
    - `bronze_transacoes`: Eventos transacionais de autorizacao e movimentacao financeira.
    - `bronze_estornos`: Eventos operacionais de cancelamento e devolucao transacional.
    - `bronze_eventos_risco`: Eventos de alerta de seguranca e prevencao a fraudes.

### Camada Silver (`novarota.silver`)

- **Objetivo:** Higienizacao, tipagem estrita, validacao declarativa de contratos de dados (*Data Contracts*) e historizacao dimensional via SCD Tipo 2.
- **Roteamento de Nao Conformidades (Quarentena):** Registros com desvios contratuais (CPFs malformatados, valores de renda inconsistentes ou status operacionais invalidos) sao segregados para tabelas de quarentena sem interrupcao do processamento (*non-breaking pipeline*), enriquecidos com `motivo_quarentena` e `data_quarentena`.
- **Historico Cadastral (SCD Tipo 2):** Implementacao desacoplada no padrao Expire + Append, eliminando o erro de concorrencia `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` por meio de deduplicacao analitica por janela e deteccao de alteracao via `_hash_diff` (SHA-256).
- **Tabelas Geradas:**
    - `dim_cliente_scd2`: Dimensao historica de cooperados com colunas de vigencia (`data_inicio_vigencia`, `data_fim_vigencia`, `flag_ativo`) e hash deterministico (`_hash_diff`).
    - `silver_transacoes`: Transacoes conformadas, tipadas e deduplicadas.
    - `quarentena_clientes`: Registros cadastrais reprovados nas regras contratuais.
    - `quarentena_transacoes`: Transacoes com inconsistencias operacionais retidas para auditoria.

### Camada Gold (`novarota.gold`)

- **Objetivo:** Materializacao de Data Products analiticos de alto desempenho orientados a consumo executivo, relatorios operacionais, investigacao de fraude e treinamento de modelos de Machine Learning.
- **Resolucao Temporal e Regras de Negocio:**
    - Resolucao relacional em cascata: `transacao -> cartao -> conta -> cooperado`.
    - *Point-in-time join* dimensional: vinculo entre a transacao e a versao cadastral do cooperado que estava em vigencia na data exata da transacao (`data_transacao >= data_inicio_vigencia AND data_transacao < coalesce(data_fim_vigencia, '9999-12-31')`).
    - Tratamento de Estornos: Abatimento integral no calculo contabilidade liquida (`valor_liquido = 0.00` para transacoes com flag de estorno confirmado), preservando `valor_bruto` para conciliacao.
    - Liquid Clustering: Otimizacao de I/O em `gold_fato_transacao` atraves de agrupamento multidimensional (`CLUSTER BY (data_referencia, id_cliente)`) com compactacao via `OPTIMIZE`.
- **Tabelas Geradas:**
    - `gold_fato_transacao`: Tabela de fatos com grao transacional unitario, snapshots cadastrais historicos e flags de risco e estorno consolidados.
    - `gold_cliente_mes`: Indicadores agregados mensais por cooperado (volumetria bruta, volume liquido, ticket medio e canais utilizados).
    - `gold_indicadores_risco`: Painel consolidado mensal por canal avaliando taxas de chargeback, proporcao de estornos e concentracao de alertas de risco.
    - `gold_features_cliente`: Feature store analitica com metricas comportamentais (desvio padrao amostral de gastos, razao de recusa, gasto maximo e recencia).

## 2- Diagrama de Linhagem e Dependencias (DAG)

```
                                  [Fontes Transacionais / CDC]
                                                |
                                                v Auto Loader (cloudFiles)
                                ┌───────────────────────────────┐
                                │ novarota.bronze.*             │
                                │  - bronze_clientes_cdc        │
                                │  - bronze_contas_cdc          │
                                │  - bronze_cartoes_cdc         │
                                │  - bronze_transacoes          │
                                │  - bronze_estornos            │
                                │  - bronze_eventos_risco       │
                                └───────────────┬───────────────┘
                                                |
                                ┌───────────────┴───────────────┐
                                v                               v
                        (Contratos Validos)             (Falha Contratual)
                ┌───────────────────────────────┐ ┌───────────────────────────────┐
                │ novarota.silver.*             │ │ novarota.silver.*             │
                │  - dim_cliente_scd2           │ │  - quarentena_clientes        │
                │  - silver_transacoes          │ │  - quarentena_transacoes      │
                └───────────────┬───────────────┘ └───────────────────────────────┘
                                |
                                v Resolucao de Chaves, Point-in-Time Join & Agregacoes
                ┌─────────────────────────────────────────────────────────────────┐
                │ novarota.gold.*                                                 │
                │  - gold_fato_transacao        (Clusterizada: data, id_cliente) │
                │  - gold_cliente_mes           (Grao mensal por cooperado)       │
                │  - gold_indicadores_risco     (Grao mensal por canal de risco)  │
                │  - gold_features_cliente      (Feature Store para ML)           │
                └─────────────────────────────────────────────────────────────────┘
```

## 3- Modelo de Governanca e Controle de Acessos (RBAC)

O controle de acesso segue o principio do menor privilegio (*Least Privilege*), delimitando o acesso as camadas com base nos perfis funcionais:

| **Papel (Role)** | **Escopo Bronze** | **Escopo Silver** | **Escopo Gold** | **Finalidade** |
| --- | --- | --- | --- | --- |
| **`data_engineers`** | ALL PRIVILEGES | ALL PRIVILEGES | ALL PRIVILEGES | Manutencao de pipelines, ingestao e transformacoes. |
| **`data_scientists`** | Sem Acesso | SELECT (dimensoes) | USAGE, SELECT | Exploracao de features e desenvolvimento de modelos. |
| **`bi_analysts`** | Sem Acesso | Sem Acesso | USAGE, SELECT | Construcao de dashboards, relatorios e consultas SQL. |

## 4- Modularizacao do Codigo (`src/`)

O projeto prioriza a separacao estrita entre logica deterministica em pacotes Python e cadernos de orquestracao, viabilizando execucao de testes unitarios desacoplados:

- **`src/bronze/`**: Metodos de ingestao incremental com Auto Loader e adicao padronizada de colunas de metadados de auditoria.
- **`src/silver/contracts.py`**: Validacao de contratos de dados e bifurcacao de registros para quarentena tecnica.
- **`src/silver/scd2.py`**: Algoritmo SCD Tipo 2 desacoplado (Expire + Append) com deteccao de mudanca via hash SHA-256.
- **`src/gold/aggregations.py`**: Resolucao dimensional em cascata, point-in-time join com SCD2, tratamento contabilidade de estornos e construcao dos quatro Data Products analiticos.
- **`tests/test_silver_contracts.py`**: Testes unitarios automatizados com fixtures compativeis com Databricks Connect e ambientes de CI/CD.