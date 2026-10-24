# Architecture Decision Records (ADR)

Este documento reúne os registos formais das decisões técnicas e arquiteturais adotadas no desenvolvimento da plataforma Lakehouse da **Cooperativa Nova Rota**.

---

## ADR 01: Adoção do Unity Catalog como Padrão Unificado de Governança
* **Estado:** Aprovado
* **Contexto:** A necessidade de manter controlos de acesso a dados (ACLs), conformidade regulatória, linhagem técnica ponta a ponta e portabilidade de código sem acoplamento a caminhos físicos de armazenamento (*storage accounts / containers*).
* **Decisão:** Organizar todas as entidades sob o namespace formal de três níveis do Unity Catalog: `novarota.<camada>.<tabela>`.
* **Consequências:**
  * Eliminação de caminhos físicos hardcoded (`abfss://` ou `s3://`) em notebooks e módulos.
  * Linhagem automática e rastreabilidade de linhagem visual acessível nativamente via Catalog Explorer.
  * Facilidade na segregação de permissões de leitura/escrita entre engenharia e equipas consumidoras de BI.

---

## ADR 02: Ingestão Incremental com Auto Loader (`cloudFiles`)
* **Estado:** Aprovado
* **Contexto:** As fontes operacionais transacionais e eventos de CDC depositam ficheiros continuamente no storage em diretórios estruturados. O método tradicional de listagem recursiva de ficheiros (*directory listing*) degrada o tempo de processamento à medida que o volume histórico cresce.
* **Decisão:** Implementar a ingestão da Camada Bronze através do Databricks Auto Loader (`format("cloudFiles")`), com gestão contínua de checkpoints e inferência controlada de esquema.
* **Consequências:**
  * Deteção de novos ficheiros orientada por logs e notificações de eventos de storage com latência mínima.
  * Proteção nativa contra mutações estruturais imprevistas na origem através da coluna técnica `_rescued_data`.
  * Garantia de semântica de entrega *exactly-once* combinada com as transações ACID do Delta Lake.

---

## ADR 03: SCD Tipo 2 no Padrão "Expire + Append" (Eliminação de Conflitos de Merge)
* **Estado:** Aprovado
* **Contexto:**
  Ao processar lotes de CDC com múltiplas atualizações para o mesmo cooperado dentro da mesma micro-tarefa, a execução de um comando `MERGE` unificado causava a falha crítica:
  `[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched and attempted to modify the same target row`.
  O erro ocorre porque a semântica de Merge do Delta Lake não permite que múltiplas linhas da fonte alterem a mesma linha de destino na mesma transação.
* **Decisão:**
  Desacoplar o processamento da dimensão histórica em duas etapas atómicas e determinísticas:
  1. **Desduplicação de Entrada:** Aplicação de janela temporal analítica (`Window.partitionBy("id_cliente").orderBy(data_atualizacao.desc())`) para manter unicamente o estado mais recente de cada cliente dentro do lote.
  2. **Passo 1 (Merge de Expiração 1x1):** O comando `MERGE` executa estritamente a expiração (`flag_ativo = false`, `data_fim_vigencia = ts`) dos registos ativos que sofreram alteração nos atributos de negócio (avaliados via `_hash_diff`). Se não houver alterações, a chamada de Merge é omitida.
  3. **Passo 2 (Append de Novas Versões):** Inserção atómica e direta das novas versões ativas através de `mode("append")` na tabela Delta.
* **Consequências:**
  * Resolução definitiva do erro `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`.
  * Redução substancial do tempo de processamento da tarefa Silver (de 6m 44s para 1m 32s).
  * Idempotência rigorosa: execuções consecutivas com a mesma partição de dados não geram linhas duplicadas nem falsas versões históricas.

---

## ADR 04: Resolução Temporal da Linhagem Referencial na Camada Gold
* **Estado:** Aprovado
* **Contexto:**
  Os eventos transacionais higienizados em `silver_transacoes` referenciam unicamente a chave do cartão (`id_cartao`). A atribuição de métricas ao cooperado (`id_cliente`) exige a resolução em cascata através das tabelas de histórico CDC `bronze_cartoes_cdc` e `bronze_contas_cdc`.
* **Decisão:**
  Construir uma visão de relacionamento desduplicada por janela analítica (`row_number() OVER (PARTITION BY <chave> ORDER BY data_atualizacao DESC)`) sobre cartões e contas antes da junção com as transações consolidadas.
* **Consequências:**
  * Prevenção de produtos cartesianos e garantia de integridade referencial nas métricas agregadas.
  * Consistência nos valores de liquidez financeira e volumetria transacionada em `gold_metricas_cooperados`.

---

## ADR 05: Orquestração Declarativa via Databricks Workflows
* **Estado:** Aprovado
* **Contexto:** Necessidade de gerir as dependências do pipeline Medalhão de ponta a ponta sem recorrer a ferramentas externas pesadas e com total integração ao repositório Git.
* **Decisão:** Configurar um DAG sequencial no Databricks Workflows (`bronze_ingestion ➔ silver_processing ➔ gold_data_product`), versionado declarativamente em YAML e apontando diretamente para a ramificação `develop`.
* **Consequências:**
  * Execução controlada com bloqueio de etapas posteriores em caso de falha a montante.
  * Observabilidade visual integrada do estado de cada tarefa diretamente na interface do Databricks.
  * Suporte nativo a targets de performance otimizada (*Serverless / Performance Optimized*).