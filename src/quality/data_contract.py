from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Tuple, Dict, Any


def validate_contract_clientes(
    df: DataFrame, rules: Dict[str, Any]
) -> Tuple[DataFrame, DataFrame]:
    """
    regras declarativas de qualidade para a entidade em clientes
    encaminha registos com violacoes para a quarentena com motivo auditavel
    """
    cpf_len = rules.get("cpf_length", 11)
    min_inc = rules.get("min_income", 0.01)
    valid_segs = rules.get(
        "valid_segments", ["VAREJO", "ALTA_RENDA", "AGRO", "EMPRESARIAL"]
    )

    cond_cpf = (F.col("cpf").isNull()) | (
        F.length(F.regexp_replace(F.col("cpf"), "[^0-9]", "")) != cpf_len
    )
    cond_renda = (F.col("renda").isNull()) | (
        F.col("renda").cast("decimal(15,2)") < min_inc
    )
    cond_seg = (~F.col("segmento").isin(valid_segs)) | (F.col("segmento").isNull())

    df_flagged = df.withColumn(
        "_motivo_quarentena",
        F.when(cond_cpf, F.lit("ERR_INVALID_CPF"))
        .when(cond_renda, F.lit("ERR_INVALID_INCOME"))
        .when(cond_seg, F.lit("ERR_INVALID_SEGMENT"))
        .otherwise(F.lit(None)),
    )

    df_validos = df_flagged.filter(F.col("_motivo_quarentena").isNull()).drop(
        "_motivo_quarentena"
    )
    df_quarentena = df_flagged.filter(
        F.col("_motivo_quarentena").isNotNull()
    ).withColumn("_data_quarentena", F.current_timestamp())

    return df_validos, df_quarentena


def validate_contract_transacoes(
    df: DataFrame, rules: Dict[str, Any]
) -> Tuple[DataFrame, DataFrame]:
    """
    aplica regras declarativas de qualidade para a entidade transacoes
    """
    min_amount = rules.get("min_amount", 0.01)
    valid_channels = rules.get(
        "valid_channels", ["POS", "ECOMMERCE", "ATM", "RECORRENTE"]
    )

    cond_val = (F.col("valor").isNull()) | (
        F.col("valor").cast("decimal(15,2)") < min_amount
    )
    cond_can = (~F.col("canal").isin(valid_channels)) | (F.col("canal").isNull())

    df_flagged = df.withColumn(
        "_motivo_quarentena",
        F.when(cond_val, F.lit("ERR_INVALID_AMOUNT"))
        .when(cond_can, F.lit("ERR_INVALID_CHANNEL"))
        .otherwise(F.lit(None)),
    )

    df_validos = df_flagged.filter(F.col("_motivo_quarentena").isNull()).drop(
        "_motivo_quarentena"
    )
    df_quarentena = df_flagged.filter(
        F.col("_motivo_quarentena").isNotNull()
    ).withColumn("_data_quarentena", F.current_timestamp())

    return df_validos, df_quarentena
