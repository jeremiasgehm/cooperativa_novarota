from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def validate_contract_clientes(df: DataFrame, rules: list) -> tuple[DataFrame, DataFrame]:
    """
    avalia predicados booleanos contratuais sobre o DataFrame de clientes
    Retorna tupla de (df_validos, df_quarentena)
    """
    condicao_valida = F.lit(True)
    for rule_expr in rules:
        condicao_valida = condicao_valida & F.expr(rule_expr)

    df_validos = df.filter(condicao_valida)
    
    df_quarentena = (
        df.filter(~condicao_valida)
        .withColumn("data_quarentena", F.current_timestamp())
        .withColumn("motivo_quarentena", F.lit("VIOLACAO_CONTRATO_DADOS"))
    )

    return df_validos, df_quarentena