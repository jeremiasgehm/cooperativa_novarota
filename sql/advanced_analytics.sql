-- =========================================================================================
-- A1: Ranking dos Cooperados com maior volume transacionado por Segmento 
-- =========================================================================================
WITH cooperados_ranqueados AS (
    SELECT
        id_cliente,
        nome,
        segmento,
        estado,
        cidade,
        total_transacoes,
        volume_total_transacionado,
        ticket_medio,
        DENSE_RANK() OVER (
            PARTITION BY segmento 
            ORDER BY volume_total_transacionado DESC
        ) AS rank_segmento
    FROM novarota.gold.gold_metricas_cooperados
    WHERE volume_total_transacionado > 0
)
SELECT 
    rank_segmento,
    segmento,
    nome,
    id_cliente,
    estado,
    total_transacoes,
    volume_total_transacionado,
    ticket_medio
FROM cooperados_ranqueados
WHERE rank_segmento <= 5
ORDER BY segmento, rank_segmento;


-- =========================================================================================
-- A2: distribuicao % de liquidez e taaxa de estorno por canal
-- =========================================================================================
WITH metricas_por_canal AS (
    SELECT
        canal,
        SUM(CASE WHEN status_transacao = 'APROVADA' THEN quantidade_eventos ELSE 0 END) AS total_aprovadas,
        SUM(CASE WHEN status_transacao = 'ESTORNADA' THEN quantidade_eventos ELSE 0 END) AS total_estornadas,
        SUM(valor_total_liquido) AS volume_financeiro_liquido
    FROM novarota.gold.gold_desempenho_canais
    GROUP BY canal
)
SELECT
    canal,
    total_aprovadas,
    total_estornadas,
    ROUND(
        (total_estornadas * 100.0) / NULLIF((total_aprovadas + total_estornadas), 0), 
        2
    ) AS taxa_estorno_pct,
    volume_financeiro_liquido,
    ROUND(
        (volume_financeiro_liquido * 100.0) / SUM(volume_financeiro_liquido) OVER (),
        2
    ) AS share_volume_total_pct
FROM metricas_por_canal
ORDER BY volume_financeiro_liquido DESC;


-- =========================================================================================
-- A3: cohort e tendencia de engajamento de novos Cooperados
-- cruza a vigencia inicial com o comportamento transacional na camada refinada
-- =========================================================================================
SELECT
    date_format(data_inicio_vigencia, 'yyyy-MM') AS safra_adesao,
    segmento,
    COUNT(DISTINCT id_cliente) AS cooperados_ativos,
    ROUND(AVG(ticket_medio), 2) AS ticket_medio_safra,
    ROUND(SUM(volume_total_transacionado), 2) AS volume_consolidado_safra
FROM novarota.gold.gold_metricas_cooperados cli
JOIN novarota.silver.dim_cliente_scd2 dim
    ON cli.id_cliente = dim.id_cliente AND dim.flag_ativo = TRUE
GROUP BY 1, 2
ORDER BY safra_adesao DESC, volume_consolidado_safra DESC;

-- -------------------------------------------------------------------------------------
-- A4: acompanhar crescimento e retenção do cooperado.
-- -------------------------------------------------------------------------------------
WITH metricas_mensais AS (
    SELECT 
        id_cliente,
        ano_mes,
        volume_liquido,
        LAG(volume_liquido, 1) OVER (
            PARTITION BY id_cliente 
            ORDER BY ano_mes ASC
        ) AS volume_mes_anterior
    FROM novarota.gold.gold_cliente_mes
)
SELECT 
    id_cliente,
    ano_mes,
    volume_liquido,
    volume_mes_anterior,
    ROUND(volume_liquido - volume_mes_anterior, 2) AS diferenca_absoluta,
    ROUND(
        ((volume_liquido - volume_mes_anterior) / NULLIF(volume_mes_anterior, 0)) * 100, 
        2
    ) AS variacao_percentual_mom
FROM metricas_mensais
ORDER BY id_cliente, ano_mes;


-- -------------------------------------------------------------------------------------
-- A5: identificar o canal preferido de entrada e o canal mais recente do cooperado.
-- -------------------------------------------------------------------------------------
SELECT DISTINCT
    id_cliente,
    FIRST_VALUE(canal) OVER (
        PARTITION BY id_cliente 
        ORDER BY data_transacao ASC 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS primeiro_canal_utilizado,
    LAST_VALUE(canal) OVER (
        PARTITION BY id_cliente 
        ORDER BY data_transacao ASC 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS ultimo_canal_utilizado,
    FIRST_VALUE(data_transacao) OVER (
        PARTITION BY id_cliente 
        ORDER BY data_transacao ASC 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS data_primeira_transacao,
    LAST_VALUE(data_transacao) OVER (
        PARTITION BY id_cliente 
        ORDER BY data_transacao ASC 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS data_ultima_transacao
FROM novarota.gold.gold_fato_transacao
WHERE status_transacao = 'APROVADA';


-- -------------------------------------------------------------------------------------
-- A6: classificar os cooperados em decis de valor financeiro
-- -------------------------------------------------------------------------------------
SELECT 
    id_cliente,
    nome,
    segmento,
    renda,
    feat_volume_total_transacionado,
    NTILE(10) OVER (
        ORDER BY feat_volume_total_transacionado DESC
    ) AS decil_faturamento,
    ROUND(
        PERCENT_RANK() OVER (
            ORDER BY feat_volume_total_transacionado ASC
        ) * 100, 
        2
    ) AS percentil_ranking_gasto
FROM novarota.gold.gold_features_cliente;


-- -------------------------------------------------------------------------------------
-- A7: Identificar compras com valor superior a 3 vezes a média histOrica do cooperado
-- -------------------------------------------------------------------------------------
WITH estatisticas_cliente AS (
    SELECT 
        id_transacao,
        id_cliente,
        data_transacao,
        valor_bruto,
        AVG(valor_bruto) OVER (PARTITION BY id_cliente) AS media_historica_cliente,
        STDDEV(valor_bruto) OVER (PARTITION BY id_cliente) AS desvio_padrao_cliente
    FROM novarota.gold.gold_fato_transacao
    WHERE status_transacao = 'APROVADA'
)
SELECT 
    id_transacao,
    id_cliente,
    data_transacao,
    valor_bruto,
    ROUND(media_historica_cliente, 2) AS media_cliente,
    ROUND(valor_bruto / NULLIF(media_historica_cliente, 0), 2) AS multiplicador_media,
    'ANOMALIA_VALOR_ELEVADO' AS diagnostico_risco
FROM estatisticas_cliente
WHERE valor_bruto > (media_historica_cliente * 3)
  AND valor_bruto > 100.00
ORDER BY multiplicador_media DESC;


-- -------------------------------------------------------------------------------------
-- a8: Comparar os gastos medios do cooperado contra seus pares regionais.
-- -------------------------------------------------------------------------------------
WITH benchmark_cidade_segmento AS (
    SELECT 
        id_cliente,
        cidade_cooperado_snapshot AS cidade,
        segmento_cooperado_snapshot AS segmento,
        valor_liquido,
        AVG(valor_liquido) OVER (
            PARTITION BY cidade_cooperado_snapshot, segmento_cooperado_snapshot
        ) AS media_cidade_segmento
    FROM novarota.gold.gold_fato_transacao
    WHERE status_transacao = 'APROVADA'
)
SELECT 
    id_cliente,
    cidade,
    segmento,
    ROUND(AVG(valor_liquido), 2) AS media_gasto_cooperado,
    ROUND(AVG(media_cidade_segmento), 2) AS media_peer_group,
    ROUND(
        AVG(valor_liquido) - AVG(media_cidade_segmento), 
        2
    ) AS diferenca_vs_benchmark
FROM benchmark_cidade_segmento
GROUP BY id_cliente, cidade, segmento
ORDER BY diferenca_vs_benchmark DESC;


-- -------------------------------------------------------------------------------------
-- A9: Demonstrar fluencia em MERGE INTO declarativo SQL para cargas idempotentes
-- -------------------------------------------------------------------------------------
MERGE INTO novarota.gold.gold_cliente_mes AS target
USING (
    SELECT 
        id_cliente,
        ano_mes,
        COUNT(id_transacao) AS total_transacoes,
        COUNT(CASE WHEN status_transacao = 'APROVADA' THEN 1 END) AS transacoes_aprovadas,
        COUNT(CASE WHEN status_transacao = 'NEGADA' THEN 1 END) AS transacoes_negadas,
        COUNT(CASE WHEN flag_estornado = TRUE THEN 1 END) AS transacoes_estornadas,
        ROUND(SUM(valor_bruto), 2) AS volume_bruto,
        ROUND(SUM(valor_liquido), 2) AS volume_liquido,
        ROUND(AVG(CASE WHEN status_transacao = 'APROVADA' THEN valor_liquido END), 2) AS ticket_medio_aprovado,
        COUNT(DISTINCT estabelecimento) AS estabelecimentos_distintos,
        COUNT(DISTINCT canal) AS canais_utilizados,
        MAX(data_transacao) AS ultima_transacao_mes,
        CURRENT_TIMESTAMP() AS data_processamento_ouro
    FROM novarota.gold.gold_fato_transacao
    WHERE data_referencia >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY id_cliente, ano_mes
) AS source
ON target.id_cliente = source.id_cliente 
AND target.ano_mes = source.ano_mes
WHEN MATCHED THEN
    UPDATE SET 
        target.total_transacoes = source.total_transacoes,
        target.transacoes_aprovadas = source.transacoes_aprovadas,
        target.transacoes_negadas = source.transacoes_negadas,
        target.transacoes_estornadas = source.transacoes_estornadas,
        target.volume_bruto = source.volume_bruto,
        target.volume_liquido = source.volume_liquido,
        target.ticket_medio_aprovado = source.ticket_medio_aprovado,
        target.estabelecimentos_distintos = source.estabelecimentos_distintos,
        target.canais_utilizados = source.canais_utilizados,
        target.ultima_transacao_mes = source.ultima_transacao_mes,
        target.data_processamento_ouro = source.data_processamento_ouro
WHEN NOT MATCHED THEN
    INSERT *;