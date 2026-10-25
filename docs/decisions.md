# Architecture Decision Records (ADR)

Este documento reúne os registros formais das decisões técnicas e arquiteturais adotadas no desenvolvimento da plataforma Lakehouse da Cooperativa Nova Rota.

---

## ADR 01: Adoção do Unity Catalog como Padrão Unificado de Governança
* **Estado:** Aprovado
* **Contexto:** Necessidade de manter controles de acesso a dados (ACLs), conformidade regulatória, linhagem técnica ponta a ponta e portabilidade de código sem acoplamento a caminhos físicos de armazenamento (*storage accounts / containers*).
* **Decisão:** Organizar todas as entidades sob o namespace formal de três níveis do Unity Catalog: `novarota.<camada>.<tabela>`.
* **Consequências:**
  * Eliminação de caminhos físicos hardcoded (`abfss://` ou `s3://`) em notebooks e módulos.
  * Linhagem automática e rastreabilidade visual acessível nativamente via Catalog Explorer.
  * Facilidade na segregação de permissões de leitura/escrita entre engenharia e times consumidores de BI/Analytics via SQL Grants padronizados.

---

## ADR 02: Ingestão Incremental com Auto Loader (`cloudFiles`)
* **Estado:** Aprovado
* **Contexto:** As fontes operacionais transacionais e eventos de CDC depositam arquivos continuamente no storage em diretórios estruturados. O método tradicional de listagem recursiva de arquivos (*directory listing*) degrada o tempo de processamento à medida que o volume histórico cresce.
* **Decisão:** Implementar a ingestão da Camada Bronze através do Databricks Auto Loader (`format("cloudFiles")`), com gestão contínua de checkpoints e inferência controlada de esquema.
* **Consequências:**
  * Detecção de novos arquivos orientada por logs e notificações de eventos de storage com latência mínima.
  * Proteção nativa contra mutações estruturais imprevistas na origem através da coluna técnica `_rescued_data`.
  * Garantia de semântica de entrega *exactly-once* combinada com as transações ACID do Delta Lake.

---

## ADR 03: SCD Tipo 2 no Padrão Expire + Append (Eliminação de Conflitos de Merge)
* **Estado:** Aprovado
* **Contexto:** Ao processar lotes de CDC com múltiplas atualizações para o mesmo cooperado dentro do mesmo micro-lote, a execução de um comando `MERGE` unificado causava a falha crítica: `[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched and attempted to modify the same target row`. A semântica de Merge do Delta Lake não permite que múltiplas linhas da fonte alterem a mesma linha de destino na mesma transação.
* **Decisão:** Desacoplar o processamento da dimensão histórica em etapas atômicas e determinísticas:
  1. **Deduplicação de Entrada:** Aplicação de janela temporal analítica (`Window.partitionBy("id_cliente").orderBy(data_atualizacao.desc())`) para reter unicamente o estado mais recente de cada cliente dentro do lote.
  2. **Passo 1 (Merge de Expiração 1x1):** O comando `MERGE` executa estritamente a expiração (`flag_ativo = false`, `data_fim_vigencia = ts`) dos registros ativos que sofreram alteração nos atributos de negócio (avaliados via `_hash_diff`). Se não houver alterações, a chamada de Merge é omitida.
  3. **Passo 2 (Append de Novas Versões):** Inserção atômica e direta das novas versões ativas através de `mode("append")` na tabela Delta.
* **Consequências:**
  * Resolução definitiva do erro `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`.
  * Redução substancial do tempo de processamento da tarefa Silver (de 6m 44s para 1m 32s).
  * Idempotência rigorosa: execuções consecutivas com a mesma partição de dados não geram linhas duplicadas nem falsas versões históricas.

---

## ADR 04: Modelagem Dimensional Gold, Resolução Temporal e Tratamento de Estornos
* **Estado:** Aprovado
* **Contexto:** A camada Gold exige a materialização de múltiplos produtos de dados analíticos com diferentes granularidades. As transações referenciam apenas `id_cartao` e precisam refletir:
  1. A associação relacional em cascata (`transacao -> cartao -> conta -> cooperado`).
  2. A versão histórica correta do cooperado vigente no momento exato em que a transação ocorreu (*point-in-time join* com SCD2).
  3. O abatimento de estornos e estorno de transações no montante financeiro líquido.
* **Decisão:**
  1. Deduplicação prévia das chaves de vínculo mais recentes em `bronze_cartoes_cdc` e `bronze_contas_cdc`.
  2. Agregação prévia de estornos e eventos de risco por `id_transacao` com colunas de nomes únicos para evitar colisões de aliases no PySpark.
  3. Realização de *point-in-time join* com `dim_cliente_scd2` validando `data_transacao >= data_inicio_vigencia AND data_transacao < coalesce(data_fim_vigencia, '9999-12-31')`.
  4. Materialização de 4 Data Products: `gold_fato_transacao` (grão transacional), `gold_cliente_mes` (comportamento mensal), `gold_indicadores_risco` (painel de fraude/chargeback) e `gold_features_cliente` (feature store analítica).
* **Consequências:**
  * Consistência analítica total sem dispersão por duplicidade em chaves relacionais.
  * Exatidão no cálculo de `valor_liquido = 0.00` para transações estornadas, preservando o valor contábil bruto para auditoria.

---

## ADR 05: Orquestração Declarativa via Databricks Workflows
* **Estado:** Aprovado
* **Contexto:** Necessidade de gerenciar as dependências do pipeline Medalhão de ponta a ponta sem ferramentas externas adicionais e com rastreabilidade integrada ao repositório Git.
* **Decisão:** Configurar um DAG sequencial no Databricks Workflows (`bronze_ingestion -> silver_processing -> gold_data_product`), versionado declarativamente em YAML e parametrizado para execução a partir da branch `develop`.
* **Consequências:**
  * Execução controlada com bloqueio automático de etapas posteriores em caso de falha a montante.
  * Observabilidade visual integrada do estado de cada tarefa diretamente na interface do Databricks.
  * Suporte nativo a targets de performance otimizada (*Performance Optimized*).

---

## ADR 06: Otimização de Leitura com Liquid Clustering na Fato Transacional
* **Estado:** Aprovado
* **Contexto:** A tabela `gold_fato_transacao` acumula grande volume analítico e é frequentemente consultada por data e por cooperado. O particionamento em diretórios rígidos por dia geraria um número excessivo de arquivos pequenos (*small files*), degradando o I/O em consultas de faixa temporal.
* **Decisão:** Aplicar **Liquid Clustering** nativo do Delta Lake na tabela fato: `CLUSTER BY (data_referencia, id_cliente)`, complementado por rotinas de compactação física via comando `OPTIMIZE`.
* **Consequências:**
  * Eliminação do overhead de criação de diretórios vazios ou pequenos arquivos.
  * Otimização automática de data-skipping tanto em filtros temporais quanto em agregações por cooperado.

---

## ADR 07: Observabilidade Proativa e Threshold de Quarentena
* **Estado:** Aprovado
* **Contexto:** A quarentena técnica garante que falhas de contrato não quebrem a esteira (*non-breaking pipeline*). Contudo, se a fonte transacional sofrer uma alteração de layout não comunicada, a volumetria da quarentena pode crescer silenciosamente sem notificação da engenharia.
* **Decisão:** Implementar verificação de SLA de qualidade de dados via query de monitoramento no Databricks SQL Alerts e emissão de avisos operacionais estruturados quando a taxa de linhas rejeitadas ultrapassar 10% do lote total ingerido no dia.
* **Consequências:**
  * Detecção antecipada de anomalias em contratos de dados antes que dados inconsistentes impactem os relatórios de negócio.
  * Histórico rastreável de taxas de conformidade por lote processado.