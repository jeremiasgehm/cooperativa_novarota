import pytest
from pyspark.sql import SparkSession
from src.quality.data_contract import (
    validate_contract_clientes,
    validate_contract_transacoes,
)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("TestNovaRotaSilver").getOrCreate()


def test_cliente_cpf_invalido_deve_ir_para_quarentena(spark):
    rules = {
        "cpf_length": 11,
        "min_income": 0.01,
        "valid_segments": ["VAREJO", "ALTA_RENDA", "AGRO", "EMPRESARIAL"],
    }
    sample_data = [
        ("CLI_001", "12345678901", 3500.00, "VAREJO"),                      # Válido
        ("CLI_002", "123", 5000.00, "VAREJO"),                              # CPF inválido (tamanho)
        ("CLI_003", "12345678901", -50.00, "VAREJO"),                       # Renda inválida
        ("CLI_004", "12345678901", 1000.00, "DESCONHECIDO"),                # Segmento inválido
    ]
    df = spark.createDataFrame(sample_data, ["id_cliente", "cpf", "renda", "segmento"])

    val, quar = validate_contract_clientes(df, rules)

    assert val.count() == 1
    assert quar.count() == 3


def test_transacao_valor_negativo_vai_para_quarentena(spark):
    rules = {"min_amount": 0.01, "valid_channels": ["POS", "ECOMMERCE"]}
    sample_data = [
        ("TX_001", 150.00, "POS"),
        ("TX_002", -10.00, "POS"),
        ("TX_003", 200.00, "INVALID_CHANNEL"),
    ]
    df = spark.createDataFrame(sample_data, ["id_transacao", "valor", "canal"])

    val, quar = validate_contract_transacoes(df, rules)

    assert val.count() == 1
    assert quar.count() == 2
