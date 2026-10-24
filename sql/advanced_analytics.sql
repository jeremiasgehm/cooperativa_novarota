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