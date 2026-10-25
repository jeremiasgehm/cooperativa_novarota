# sla
total_registros = df_entrada.count()
total_quarentena = df_quarentena.count()

if total_registros > 0:
    taxa_rejeicao = (total_quarentena / total_registros) * 100
    print(
        f"[OBSERVABILIDADE] Taxa de rejeição na quarentena: {taxa_rejeicao:.2f}% ({total_quarentena}/{total_registros})"
    )

    # trashodl segurança, alerta caso ultrapasse o limite aceitavel
    LIMITE_CRITICO_QUARENTENA = 20.0
    if taxa_rejeicao > LIMITE_CRITICO_QUARENTENA:
        import warnings

        warnings.warn(
            f"ALERTA OPERACIONAL: A quarentena atingiu {taxa_rejeicao:.2f}%, "
            f"superando o threshold de {LIMITE_CRITICO_QUARENTENA}%. "
            "Possível quebra de contrato na origem!"
        )
