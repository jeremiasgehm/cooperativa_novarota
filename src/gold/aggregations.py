# src/gold/aggregations.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def get_latest_cdc_records(
    df: DataFrame, key_col: str, order_col: str = "data_atualizacao"
) -> DataFrame:
    """Retorna apenas o registro mais recente por chave primária de uma tabela CDC."""
    w = Window.partitionBy(key_col).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def build_gold_metricas_cooperados(
    df_dim_cliente: DataFrame,
    df_transacoes: DataFrame,
    df_cartoes: DataFrame,
    df_contas: DataFrame,
) -> DataFrame:
    """
    consolidando volumetria e liquidez
    id_cartao -> id_conta -> id_cliente.
    """
    cartoes_recentes = get_latest_cdc_records(df_cartoes, "id_cartao").select(
        "id_cartao", "id_conta"
    )
    contas_recentes = get_latest_cdc_records(df_contas, "id_conta").select(
        "id_conta", "id_cliente"
    )

    mapa_cartao_cliente = (
        cartoes_recentes.join(contas_recentes, on="id_conta", how="inner")
        .select("id_cartao", "id_cliente")
        .distinct()
    )

    tx_com_cliente = df_transacoes.filter(F.col("status_transacao") == "APROVADA").join(
        mapa_cartao_cliente, on="id_cartao", how="inner"
    )

    tx_agrupadas = tx_com_cliente.groupBy("id_cliente").agg(
        F.count("id_transacao").alias("total_transacoes"),
        F.sum("valor_liquido")
        .cast("decimal(18,2)")
        .alias("volume_total_transacionado"),
        F.avg("valor_liquido").cast("decimal(18,2)").alias("ticket_medio"),
        F.max("data_transacao").alias("ultima_transacao"),
    )

    clientes_ativos = df_dim_cliente.filter(F.col("flag_ativo") == True)

    gold_cooperados = clientes_ativos.join(
        tx_agrupadas, on="id_cliente", how="left"
    ).select(
        F.col("id_cliente"),
        F.col("nome"),
        F.col("segmento"),
        F.col("estado"),
        F.col("cidade"),
        F.coalesce(F.col("total_transacoes"), F.lit(0)).alias("total_transacoes"),
        F.coalesce(F.col("volume_total_transacionado"), F.lit(0.00)).alias(
            "volume_total_transacionado"
        ),
        F.coalesce(F.col("ticket_medio"), F.lit(0.00)).alias("ticket_medio"),
        F.col("ultima_transacao"),
        F.current_timestamp().alias("data_processamento_ouro"),
    )

    return gold_cooperados


def build_gold_desempenho_canais(df_transacoes: DataFrame) -> DataFrame:
    """
    agrega volumetria e liquidez financeira agrupadas por canal de transacao
    """
    return (
        df_transacoes.groupBy("canal", "status_transacao")
        .agg(
            F.count("id_transacao").alias("quantidade_eventos"),
            F.sum("valor_liquido").cast("decimal(18,2)").alias("valor_total_liquido"),
            F.min("data_transacao").alias("primeira_ocorrencia"),
            F.max("data_transacao").alias("ultima_ocorrencia"),
        )
        .withColumn("data_processamento_ouro", F.current_timestamp())
    )
