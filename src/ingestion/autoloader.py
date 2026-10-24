from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional, Dict


def enrich_bronze_metadata(
    df: DataFrame, batch_id: str, schema_version: str = "v1.0"
) -> DataFrame:
    """
    enriquece o df com colunas
    usa _metadata.file_path nativo do UC
    """
    data_cols = [c for c in df.columns if not c.startswith("_")]

    hash_expr = F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(c).cast("string"), F.lit("__NULL__"))
                for c in data_cols
            ],
        ),
        256,
    )

    return (
        df.withColumn("_hash_linha", hash_expr)
        .withColumn("_timestamp_ingestao", F.current_timestamp())
        .withColumn("_data_ingestao", F.to_date(F.current_timestamp()))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_schema_version", F.lit(schema_version))
        .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
    )


def ingest_autoloader_stream(
    spark: SparkSession,
    source_format: str,
    source_path: str,
    target_table: str,
    checkpoint_dir: str,
    batch_id: str,
    schema_version: str = "v1.0",
    extra_options: Optional[Dict[str, str]] = None,
):
    """
    executa a ingestao incremental usando auto loader
    """
    options = {
        "cloudFiles.format": source_format,
        "cloudFiles.schemaLocation": f"{checkpoint_dir}/schema",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
    }

    if source_format == "csv":
        options["cloudFiles.inferColumnTypes"] = "false"

    if extra_options:
        options.update(extra_options)

    stream_df = (
        spark.readStream.format("cloudFiles").options(**options).load(source_path)
    )
    enriched_df = enrich_bronze_metadata(
        stream_df, batch_id=batch_id, schema_version=schema_version
    )

    query = (
        enriched_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_dir}/data")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()
