"""
Validacao dos contratos de dados, regras de quarentena e caalculo de hash diff
Executavel tanto em clusters Databricks (Spark Connect) quanto localmente / CI
"""

import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from src.silver.contracts import validate_contract_clientes
from src.silver.scd2 import compute_hash_diff


@pytest.fixture(scope="session")
def spark():
    """
    Obtem a sessao Spark existente no Databricks (Spark Connect)
    ou instancia uma sesso local se estiver rodando em CI/CD/maquina local
    """
    # 1. Se estiver rodando dentro do Databricks / Spark Connect
    if "SPARK_REMOTE" in os.environ or "spark.remote" in os.environ:
        return SparkSession.builder.getOrCreate()

    # 2. Se já existir uma sessão ativa no driver
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    # 3. Fallback estritamente local (máquina física / GitHub Actions fora do Databricks)
    return (
        SparkSession.builder.master("local[1]")
        .appName("test-cooperativa-novarota")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )


def test_validate_contract_clientes_quarentena_segregada(spark):
    """
    Cenaario: Lote com um associado e tres associados com desvios contratuais
    (CPF fora do padrao, renda negativa e segmento fora do domnio permitido)
    validar se os registos vlidos seguem o fluxo e se os invalidos encaminhados
    para a quarentena com os devidos metadados de auditoria
    """
    schema = StructType(
        [
            StructField("id_cliente", StringType(), True),
            StructField("cpf", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("cidade", StringType(), True),
            StructField("estado", StringType(), True),
            StructField("renda", DoubleType(), True),
            StructField("segmento", StringType(), True),
        ]
    )

    dados_amostra = [
        #  o valor valido
        (
            "CLI_001",
            "12345678901",
            "Cooperado Valido",
            "Porto Alegre",
            "RS",
            7500.0,
            "AGRO",
        ),
        # cpfd incorreto s
        (
            "CLI_002",
            "123",
            "Cooperado CPF Curto",
            "Caxias do Sul",
            "RS",
            4200.0,
            "INDIVIDUAL",
        ),
        # renda negativa
        (
            "CLI_003",
            "98765432100",
            "Cooperado Renda Negativa",
            "Passo Fundo",
            "RS",
            -500.0,
            "EMPRESARIAL",
        ),
        # seg desconhecido
        (
            "CLI_004",
            "11122233344",
            "Cooperado Segmento Invalido",
            "Pelotas",
            "RS",
            12000.0,
            "GOVERNO",
        ),
    ]

    df_entrada = spark.createDataFrame(dados_amostra, schema=schema)

    rules = [
        "id_cliente IS NOT NULL",
        "length(cpf) = 11",
        "renda >= 0.0",
        "segmento IN ('AGRO', 'EMPRESARIAL', 'INDIVIDUAL', 'PRIVATE')",
    ]

    df_validos, df_quarentena = validate_contract_clientes(df_entrada, rules)

    assert df_validos.count() == 1, "Deveria haver exatamente 1 registo válido aprovado"
    assert df_quarentena.count() == 3, (
        "Deveria haver exatamente 3 registos encaminhados para a quarentena"
    )

    valido_row = df_validos.collect()[0]
    assert valido_row["id_cliente"] == "CLI_001"
    assert valido_row["segmento"] == "AGRO"

    assert "data_quarentena" in df_quarentena.columns
    assert "motivo_quarentena" in df_quarentena.columns


def test_compute_hash_diff_detecta_alteracao(spark):
    """
    dois registos com os mesmos dados cadastrais, mas um com renda alterada
    Valida se o calculo SHA-256 reflete a alteração de dados de negocio
    """
    schema = StructType(
        [
            StructField("id_cliente", StringType(), True),
            StructField("cpf", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("renda", DoubleType(), True),
            StructField("segmento", StringType(), True),
        ]
    )

    dados = [
        ("CLI_010", "12345678900", "Joao da Silva", 5000.0, "INDIVIDUAL"),
        ("CLI_010", "12345678900", "Joao da Silva", 8500.0, "INDIVIDUAL"),
    ]

    df = spark.createDataFrame(dados, schema=schema)
    cols_negocio = ["cpf", "nome", "renda", "segmento"]

    df_com_hash = compute_hash_diff(df, cols_negocio)
    hashes = [row["_hash_diff"] for row in df_com_hash.collect()]

    assert len(hashes) == 2
    assert hashes[0] != hashes[1], (
        "o _hash_diff deve mudar quando houver alteracao de atributos de negocio"
    )
