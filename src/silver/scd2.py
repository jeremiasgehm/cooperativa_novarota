from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


def compute_hash_diff(df: DataFrame, business_cols: list) -> DataFrame:
    """
    calcula hash SHA-256 dos campos de negocio para identificar alteracoes
    """
    return df.withColumn(
        "_hash_diff",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(c).cast("string"), F.lit("__NULL__"))
                    for c in business_cols
                ],
            ),
            256,
        ),
    )


def execute_scd2_merge(
    spark: SparkSession,
    target_table_name: str,
    df_updates: DataFrame,
    primary_key: str = "id_cliente",
):
    """
    SCD t2 desacoplado (Merge + Insert de novos),
    garantindo que nunca ocorra erro de múltiplas linhas
    """
    target_delta = DeltaTable.forName(spark, target_table_name)
    target_df = target_delta.toDF()

    order_col = (
        "data_atualizacao"
        if "data_atualizacao" in df_updates.columns
        else "_timestamp_ingestao"
    )
    window_spec = Window.partitionBy(primary_key).orderBy(F.col(order_col).desc())
    latest_updates = (
        df_updates.withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    active_target = target_df.filter(F.col("flag_ativo") == True)

    records_changed = (
        latest_updates.alias("src")
        .join(active_target.alias("tgt"), primary_key, "inner")
        .filter(F.col("src._hash_diff") != F.col("tgt._hash_diff"))
        .select(
            F.col(f"src.{primary_key}").alias(primary_key),
            F.col(f"src.{order_col}").alias("dt_expiracao"),
        )
    )

    (
        target_delta.alias("tgt")
        .merge(
            source=records_changed.alias("src"),
            condition=f"tgt.{primary_key} = src.{primary_key} AND tgt.flag_ativo = true",
        )
        .whenMatchedUpdate(
            set={
                "flag_ativo": F.lit(False),
                "data_fim_vigencia": F.col("src.dt_expiracao"),
            }
        )
        .execute()
    )

    existing_unchanged = (
        latest_updates.alias("src")
        .join(active_target.alias("tgt"), primary_key, "inner")
        .filter(F.col("src._hash_diff") == F.col("tgt._hash_diff"))
        .select(F.col(f"src.{primary_key}"))
    )

    records_to_insert = (
        latest_updates.alias("src")
        .join(existing_unchanged.alias("no_change"), primary_key, "left_anti")
        .withColumn("data_inicio_vigencia", F.col(order_col))
        .withColumn("data_fim_vigencia", F.lit(None).cast("timestamp"))
        .withColumn("flag_ativo", F.lit(True))
        .withColumn("data_processamento_prata", F.current_timestamp())
        .withColumn("_timestamp_ingestao", F.current_timestamp())
        .withColumn("_batch_id", F.lit("silver_scd2"))
    )

    if records_to_insert.count() > 0:
        target_cols = [c for c in spark.table(target_table_name).columns]
        records_to_insert.select(*target_cols).write.format("delta").mode(
            "append"
        ).saveAsTable(target_table_name)
