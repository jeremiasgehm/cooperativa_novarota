from pyspark.sql import DataFrame, SparkSession
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


def build_gold_fato_transacao(
    spark: SparkSession,
    catalog: str,
    schema_silver: str,
    schema_bronze: str
) -> DataFrame:
    """
    Constroi a tabela de fatos 'gold_fato_transacao'
    """
    df_transacoes = spark.table(f"{catalog}.{schema_silver}.silver_transacoes")
    df_clientes_scd2 = spark.table(f"{catalog}.{schema_silver}.dim_cliente_scd2")
    df_cartoes = spark.table(f"{catalog}.{schema_bronze}.bronze_cartoes_cdc")
    df_contas = spark.table(f"{catalog}.{schema_bronze}.bronze_contas_cdc")
    
    has_estornos = spark.catalog.tableExists(f"{catalog}.{schema_bronze}.bronze_estornos")
    df_estornos = (
        spark.table(f"{catalog}.{schema_bronze}.bronze_estornos") 
        if has_estornos 
        else spark.createDataFrame([], "id_estorno string, id_transacao string, data_estorno timestamp, motivo string")
    )
    
    has_risco = spark.catalog.tableExists(f"{catalog}.{schema_bronze}.bronze_eventos_risco")
    df_risco = (
        spark.table(f"{catalog}.{schema_bronze}.bronze_eventos_risco")
        if has_risco
        else spark.createDataFrame([], "id_evento string, id_transacao string, tipo_evento string, severidade string, data_evento timestamp")
    )

    w_cartao = Window.partitionBy("id_cartao").orderBy(F.col("data_atualizacao").desc())
    cartoes_dedup = (
        df_cartoes
        .withColumn("_rn", F.row_number().over(w_cartao))
        .filter(F.col("_rn") == 1)
        .select("id_cartao", "id_conta", "tipo_cartao", "status_cartao", "limite")
    )

    w_conta = Window.partitionBy("id_conta").orderBy(F.col("data_atualizacao").desc())
    contas_dedup = (
        df_contas
        .withColumn("_rn", F.row_number().over(w_conta))
        .filter(F.col("_rn") == 1)
        .select("id_conta", "id_cliente", "tipo_conta", "status_conta")
    )

    estornos_agg = (
        df_estornos
        .groupBy("id_transacao")
        .agg(
            F.lit(True).alias("is_estornado"),
            F.count("id_estorno").alias("qtd_estornos"),
            F.max("motivo").alias("motivo_estorno_desc")
        )
    )

    risco_agg = (
        df_risco
        .groupBy("id_transacao")
        .agg(
            F.lit(True).alias("has_risco"),
            F.max("tipo_evento").alias("tipo_evento_desc"),
            F.max("severidade").alias("severidade_desc")
        )
    )

    transacoes_base = (
        df_transacoes.alias("t")
        .join(cartoes_dedup.alias("car"), "id_cartao", "left")
        .join(contas_dedup.alias("cta"), "id_conta", "left")
        .join(estornos_agg.alias("est"), "id_transacao", "left")
        .join(risco_agg.alias("rsc"), "id_transacao", "left")
    )

    fim_vigencia_tratada = F.coalesce(
        F.col("cli.data_fim_vigencia"), 
        F.to_timestamp(F.lit("9999-12-31 23:59:59"))
    )

    condicao_join_cliente = (
        (F.col("tx.id_cliente") == F.col("cli.id_cliente")) &
        (F.col("tx.data_transacao") >= F.col("cli.data_inicio_vigencia")) &
        (F.col("tx.data_transacao") < fim_vigencia_tratada)
    )

    fato_transacao = (
        transacoes_base.alias("tx")
        .join(
            df_clientes_scd2.alias("cli"),
            condicao_join_cliente,
            "left"
        )
        .select(
            F.col("tx.id_transacao"),
            F.col("tx.id_cartao"),
            F.col("tx.id_conta"),
            F.col("tx.id_cliente"),
            F.col("tx.data_transacao"),
            F.to_date(F.col("tx.data_transacao")).alias("data_referencia"),
            F.date_format(F.col("tx.data_transacao"), "yyyy-MM").alias("ano_mes"),
            F.col("tx.valor").cast("decimal(15,2)").alias("valor_bruto"),
            F.when(
                F.coalesce(F.col("tx.is_estornado"), F.lit(False)) == True,
                F.lit(0.00)
            ).otherwise(F.col("tx.valor")).cast("decimal(15,2)").alias("valor_liquido"),
            F.col("tx.canal"),
            F.col("tx.mcc"),
            F.col("tx.estabelecimento"),
            F.col("tx.pais"),
            F.col("tx.moeda"),
            F.coalesce(F.col("tx.status_transacao"), F.lit("APROVADA")).alias("status_transacao"),
            F.coalesce(F.col("tx.is_estornado"), F.lit(False)).alias("flag_estornado"),
            F.col("tx.motivo_estorno_desc").alias("motivo_estorno"),
            F.coalesce(F.col("tx.has_risco"), F.lit(False)).alias("flag_evento_risco"),
            F.col("tx.tipo_evento_desc").alias("tipo_evento_risco"),
            F.col("tx.severidade_desc").alias("severidade_risco"),
            F.col("cli.cidade").alias("cidade_cooperado_snapshot"),
            F.col("cli.estado").alias("estado_cooperado_snapshot"),
            F.col("cli.segmento").alias("segmento_cooperado_snapshot"),
            F.current_timestamp().alias("data_processamento_ouro")
        )
    )
    return fato_transacao


def build_gold_cliente_mes(df_fato_transacao: DataFrame) -> DataFrame:
    """
    'gold_cliente_mes'
    """
    return (
        df_fato_transacao.filter(F.col("id_cliente").isNotNull())
        .groupBy("id_cliente", "ano_mes")
        .agg(
            F.count("id_transacao").alias("total_transacoes"),
            F.count(F.when(F.col("status_transacao") == "APROVADA", 1)).alias(
                "transacoes_aprovadas"
            ),
            F.count(F.when(F.col("status_transacao") == "NEGADA", 1)).alias(
                "transacoes_negadas"
            ),
            F.count(F.when(F.col("flag_estornado") == True, 1)).alias(
                "transacoes_estornadas"
            ),
            F.round(F.sum("valor_bruto"), 2).alias("volume_bruto"),
            F.round(F.sum("valor_liquido"), 2).alias("volume_liquido"),
            F.round(
                F.avg(
                    F.when(
                        F.col("status_transacao") == "APROVADA", F.col("valor_liquido")
                    )
                ),
                2,
            ).alias("ticket_medio_aprovado"),
            F.countDistinct("estabelecimento").alias("estabelecimentos_distintos"),
            F.countDistinct("canal").alias("canais_utilizados"),
            F.max("data_transacao").alias("ultima_transacao_mes"),
        )
        .withColumn("data_processamento_ouro", F.current_timestamp())
    )


def build_gold_indicadores_risco(df_fato_transacao: DataFrame) -> DataFrame:
    """
    'gold_indicadores_risco'
    """
    return (
        df_fato_transacao.groupBy("ano_mes", "canal")
        .agg(
            F.count("id_transacao").alias("total_transacoes"),
            F.count(F.when(F.col("flag_evento_risco") == True, 1)).alias(
                "total_eventos_risco"
            ),
            F.count(F.when(F.col("flag_estornado") == True, 1)).alias("total_estornos"),
            F.count(F.when(F.col("severidade_risco") == "ALTA", 1)).alias(
                "alertas_severidade_alta"
            ),
            F.round(F.sum("valor_bruto"), 2).alias("volume_total_canal"),
            F.round(
                (
                    F.count(F.when(F.col("flag_evento_risco") == True, 1))
                    / F.count("id_transacao")
                )
                * 100,
                2,
            ).alias("taxa_risco_percentual"),
            F.round(
                (
                    F.count(F.when(F.col("flag_estornado") == True, 1))
                    / F.count("id_transacao")
                )
                * 100,
                2,
            ).alias("taxa_estorno_percentual"),
        )
        .withColumn("data_processamento_ouro", F.current_timestamp())
    )


def build_gold_features_cliente(
    df_fato_transacao: DataFrame, df_clientes_scd2: DataFrame
) -> DataFrame:
    """
    'gold_features_cliente'
    """
    clientes_ativos = df_clientes_scd2.filter(F.col("flag_ativo") == True).select(
        "id_cliente", "nome", "segmento", "renda", "cidade", "estado"
    )

    features_agg = df_fato_transacao.groupBy("id_cliente").agg(
        F.count("id_transacao").alias("feat_total_transacoes_historico"),
        F.round(F.sum("valor_liquido"), 2).alias("feat_volume_total_transacionado"),
        F.round(F.avg("valor_liquido"), 2).alias("feat_ticket_medio"),
        F.round(F.stddev("valor_liquido"), 2).alias("feat_desvio_padrao_gastos"),
        F.round(F.max("valor_liquido"), 2).alias("feat_maior_transacao_unitária"),
        F.count(F.when(F.col("flag_estornado") == True, 1)).alias(
            "feat_total_estornos_sofridos"
        ),
        F.count(F.when(F.col("flag_evento_risco") == True, 1)).alias(
            "feat_total_eventos_risco_registrados"
        ),
        F.count(F.when(F.col("status_transacao") == "NEGADA", 1)).alias(
            "feat_total_transacoes_negadas"
        ),
        F.round(
            (
                F.count(F.when(F.col("status_transacao") == "NEGADA", 1))
                / F.count("id_transacao")
            ),
            4,
        ).alias("feat_taxa_recusa"),
        F.datediff(F.current_timestamp(), F.max("data_transacao")).alias(
            "feat_dias_desde_ultima_transacao"
        ),
    )

    return (
        clientes_ativos.alias("cli")
        .join(features_agg.alias("ft"), "id_cliente", "left")
        .fillna(
            {
                "feat_total_transacoes_historico": 0,
                "feat_volume_total_transacionado": 0.0,
                "feat_ticket_medio": 0.0,
                "feat_desvio_padrao_gastos": 0.0,
                "feat_maior_transacao_unitária": 0.0,
                "feat_total_estornos_sofridos": 0,
                "feat_total_eventos_risco_registrados": 0,
                "feat_total_transacoes_negadas": 0,
                "feat_taxa_recusa": 0.0,
                "feat_dias_desde_ultima_transacao": -1,
            }
        )
        .withColumn("data_calculo_features", F.current_timestamp())
    )
