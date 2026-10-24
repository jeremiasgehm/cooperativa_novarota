# Arquitetura Técnica e Governança Lakehouse

Este documento apresenta o desenho técnico da plataforma Lakehouse da **Cooperativa Nova Rota**, descrevendo a estrutura de dados no Unity Catalog, o fluxo de linhagem ponta a ponta, os mecanismos de resiliência e a modularização do código.

---

## 1. Topologia de Dados e Governança no Unity Catalog

A plataforma foi implementada utilizando o namespace de três níveis do Databricks Unity Catalog (`novarota.<camada>.<tabela>`). Essa abordagem elimina o acoplamento a caminhos físicos de armazenamento (*storage locations*) e centraliza o controlo de acessos, auditoria e linhagem técnica:

```
                           novarota 
                         (Catálogo)
       ┌──────────────────────┼──────────────────────┐
       ▼                      ▼                      ▼
    bronze                  silver                 gold
(Raw / Ingestão)       (Limpeza / SCD2)       (Data Products)
```

---

### A. Camada Bronze (`novarota.bronze`)
* **Objetivo:** Ingestão bruta, append-only e imutável das origens transacionais e streams de CDC (Change Data Capture).
* **Mecanismo:** Databricks Auto Loader (`format("cloudFiles")`), que utiliza notificações e logs de ficheiros no storage para processamento incremental eficiente.
* **Schema Evolution & Rescue:** Inclusão nativa da coluna `_rescued_data`, prevenindo a falha do pipeline caso surjam novos atributos ou formatos inesperados.
* **Metadados Técnicos de Auditoria Forense:**
  * `_timestamp_ingestao`: Timestamp UTC preciso da execução da micro-tarefa.
  * `_arquivo_origem`: Caminho completo de origem do ficheiro processado no storage.
  * `_batch_id`: Identificador da execução do job para idempotência.
  * `_hash_linha`: Hash SHA-256 do registo bruto para auditoria e rastreio de duplicados.
* **Tabelas Geradas:**
  * `bronze_clientes_cdc`: Histórico bruto de atualizações cadastrais de clientes.
  * `bronze_contas_cdc`: Histórico bruto de abertura e estado das contas.
  * `bronze_cartoes_cdc`: Relação transacional e operacional de cartões.
  * `bronze_transacoes`: Eventos transacionais de movimentação financeira.

---

### B. Camada Silver (`novarota.silver`)
* **Objetivo:** Higienização, tipagem estrita, aplicação de contratos de qualidade de dados (*Data Contracts*) e conformação dimensional histórica.
* **Roteamento de Não-Conformidades (Quarentena):** Registos com desvios contratuais (ex.: campos obrigatórios nulos, valores fora dos limites) são direcionados para tabelas de quarentena com enriquecimento de `motivo_quarentena` e `data_quarentena`.
* **Histórico Cadastral (SCD Tipo 2):** Implementação desacoplada (*Expire + Append*) para rastrear a evolução dos cooperados ao longo do tempo.
* **Tabelas Geradas:**
  * `dim_cliente_scd2`: Dimensão cooperado com colunas de vigência (`data_inicio_vigencia`, `data_fim_vigencia`, `flag_ativo`) e hash de negócio (`_hash_diff`).
  * `silver_transacoes`: Transações validadas, desduplicadas e conciliadas financeiramente.
  * `quarentena_clientes`: Registos de cooperados rejeitados pelas regras de qualidade.
  * `quarentena_transacoes`: Transações inválidas retidas para auditoria e análise de causa-raiz.

---

### C. Camada Gold (`novarota.gold`)
* **Objetivo:** Disponibilização de *Data Products* analíticos prontos para BI, análises ad-hoc e consumo direto por equipas de negócio.
* **Linhagem Referencial Complexa:** Resolução em cascata para associar eventos de transação ao cooperado correto:
  $$\text{silver\_transacoes} \xrightarrow{\text{id\_cartao}} \text{bronze\_cartoes\_cdc} \xrightarrow{\text{id\_conta}} \text{bronze\_contas_cdc} \xrightarrow{\text{id\_cliente}} \text{dim\_cliente\_scd2}$$
* **Tabelas Geradas:**
  * `gold_metricas_cooperados`: Visão analítica 360 do cooperado (total de transações aprovadas, volume financeiro líquido transacionado, ticket médio e data do último movimento).
  * `gold_desempenho_canais`: Indicadores volumétricos e liquidez por canal de operação (ATM, POS, Internet Banking, Mobile) e estado transacional.

---

## 2. Diagrama de Linhagem e Dependências (DAG)
```
                                  [Fontes de Dados / CDC]
                                            │
                                            ▼ 
                                 (Auto Loader cloudFiles)
                                ┌──────────────────────────┐
                                │ novarota.bronze.*        │
                                │  - bronze_clientes_cd    │
                                │  - bronze_contas_cdc     │
                                │  - bronze_cartoes_cdc    │
                                │  - bronze_transacoes     │
                                └────────────┬─────────────┘
                                             │
                                    ┌────────┴────────┐
                                    ▼                 ▼
                                (Validação)      (Rejeições)
                        ┌───────────────────┐ ┌─────────────────────────┐
                        │ novarota.silver   │ │ novarota.silver         │
                        │  - dim_cliente_   │ │  - quarentena_clientes  │
                        │    scd2           │ │  - quarentena_transacoes│
                        │  - silver_        │ └─────────────────────────┘
                        │    transacoes     │
                        └─────────┬─────────┘
                                  │
                                  ▼ 
                (Resolução de Chaves e Agregações)
                ┌─────────────────────────────────┐
                │ novarota.gold                   │
                │  - gold_metricas_cooperados     │
                │  - gold_desempenho_canais       │
                └─────────────────────────────────┘
```

## 3. Modularização do Código (`src/`)

O projeto prioriza a separação estrita entre lógica pura (Python) e orquestração (Notebooks), viabilizando testes unitários e reutilização de código:

* **`src/bronze/`**: Funções reutilizáveis para ingestão incremental com Auto Loader e injeção automática de metadados forenses.
* **`src/silver/contracts.py`**: Lógica de aplicação dos contratos de dados, separação de registos íntegros e roteamento para quarentena.
* **`src/silver/scd2.py`**: Algoritmo desacoplado de SCD Tipo 2 (*Expire + Append*), garantindo ausência de colisões de múltiplas linhas no Delta Lake.
* **`src/gold/aggregations.py`**: Resolução temporal da linhagem CDC e construção dos Data Products analíticos.