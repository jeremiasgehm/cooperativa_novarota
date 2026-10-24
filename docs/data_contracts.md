# Contratos de Dados e Estratégia de Quarentena

Este documento descreve as políticas contratuais de dados (*Data Contracts*), os critérios de validação de integridade e a arquitetura do mecanismo de quarentena implementado na Camada Silver.

---

## 1. Princípio de Não-Ruptura com Roteamento Seguro

Em arquiteturas de dados de missão crítica, adotar abordagens de falha imediata (*fail-fast*) que interrompem todo o pipeline ou o descarte silencioso de linhas inválidas (*silent drop*) acarreta riscos operacionais elevados:
* Interrupções afetam o processamento de dados saudáveis que dependem da execução do DAG.
* O descarte silencioso impossibilita a identificação de anomalias na origem e compromete auditorias.

Para mitigar estes riscos, foi adotado o **Padrão de Quarentena Baseado em Contratos de Dados**:
1. O lote de dados de entrada é avaliado através de regras declarativas expressas em predicados lógicos.
2. Os registos em conformidade seguem diretamente o fluxo para as tabelas analíticas Silver (`dim_cliente_scd2` e `silver_transacoes`).
3. Os registos que violam uma ou mais regras contratuais são bifurcados para tabelas de quarentena dedicadas (`quarentena_clientes` e `quarentena_transacoes`).
4. Cada linha retida em quarentena é enriquecida com:
   * `motivo_quarentena`: Descritor técnico claro da condição violada.
   * `data_quarentena`: Timestamp UTC no momento exato da triagem.

---

## 2. Especificação dos Contratos de Dados

### A. Domínio de Cooperados (`novarota.silver.quarentena_clientes`)

| Campo | Regra Contratual / Validação | Severidade | Motivo Registado |
| :--- | :--- | :--- | :--- |
| `id_cliente` | `id_cliente IS NOT NULL` | Crítica | `ID_CLIENTE_NULO` |
| `cpf` | Não nulo e formato válido (11 dígitos numéricos limpos) | Alta | `CPF_INVALIDO` |
| `renda` | `renda >= 0.0` (não pode assumir valor financeiro negativo) | Alta | `RENDA_NEGATIVA` |
| `segmento` | Pertence à lista de domínios cadastrados: `['AGRO', 'EMPRESARIAL', 'INDIVIDUAL', 'PRIVATE']` | Média | `SEGMENTO_DESCONHECIDO` |

---

### B. Domínio de Transações (`novarota.silver.quarentena_transacoes`)

| Campo | Regra Contratual / Validação | Severidade | Motivo Registado |
| :--- | :--- | :--- | :--- |
| `id_transacao` | `id_transacao IS NOT NULL` | Crítica | `ID_TRANSACAO_NULO` |
| `id_cartao` | `id_cartao IS NOT NULL` | Crítica | `ID_CARTAO_NULO` |
| `valor_transacao` | `valor_transacao > 0.00` | Alta | `VALOR_TRANSACAO_INVALIDO` |
| `data_transacao` | Não nulo e valor temporalmente plausível (`data_transacao <= current_timestamp()`) | Alta | `DATA_FUTURA_OU_NULA` |
| `status_transacao` | Domínio fechado: `['APROVADA', 'NEGADA', 'ESTORNADA']` | Média | `STATUS_INVALIDO` |

---

## 3. Implementação Técnica (`src/silver/contracts.py`)

A separação dos registos válidos dos não-conformes é realizada através de operações vetorizadas de DataFrames no PySpark, garantindo alta performance:

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def validate_contract_clientes(df: DataFrame, rules: list) -> tuple[DataFrame, DataFrame]:
    """
    Avalia predicados booleanos contratuais sobre a fonte de cooperados.
    Retorna uma tupla contendo (df_validos, df_quarentena).
    """
    # Constrói condição lógica cumulativa de validação
    condicao_valida = F.lit(True)
    for rule_expr in rules:
        condicao_valida = condicao_valida & F.expr(rule_expr)

    # Bifurcação dos fluxos
    df_validos = df.filter(condicao_valida)
    
    df_quarentena = (
        df.filter(~condicao_valida)
        .withColumn("data_quarentena", F.current_timestamp())
        .withColumn("motivo_quarentena", F.lit("VIOLACAO_CONTRATO_DADOS"))
    )

    return df_validos, df_quarentena
```

## 4. Governança e Ciclo de Vida da Quarentena

- Auditoria Contínua: As tabelas de quarentena residem no esquema novarota.silver e mantêm o histórico das violações, permitindo à equipa de dados avaliar a qualidade dos provedores de origem.

- Reprocessamento: Caso a origem saneie registos corrompidos ou execute correções em lote, os registos podem ser reprocessados a partir da camada Bronze sem comprometer a consistência da dimensão ```dim_cliente_scd2```.