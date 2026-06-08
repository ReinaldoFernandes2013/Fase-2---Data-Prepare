"""
SQUAD: Testes Automatizados de Qualidade (QA/MLOps)
OBJETIVO: Validar se as regras de corte estatístico da Pesquisa Alfabetiza Brasil
          estão sendo aplicadas com precisão cirúrgica de 743 pontos.
"""

import pytest

# Simulação da função de classificação que roda no pipeline do Leonardo e no seu Dashboard
def classificar_status_saeb(proficiencia):
    if proficiencia >= 743.0:
        return 'Alfabetizado (Metas Saeb)'
    return 'Atenção / Abaixo da Meta'

def test_deve_classificar_como_alfabetizado_acima_do_ponto_de_corte():
    # Cenário: Nota maior que o corte (Ex: Franca com 765.2)
    resultado = classificar_status_saeb(765.2)
    assert resultado == 'Alfabetizado (Metas Saeb)'

def test_deve_classificar_como_atencao_abaixo_do_ponto_de_corte():
    # Cenário: Nota menor que o corte (Ex: Campinas com 729.5)
    resultado = classificar_status_saeb(729.5)
    assert resultado == 'Atenção / Abaixo da Meta'

def test_caso_limite_exato_dos_743_pontos():
    # Cenário Crítico: Exatamente no limite do corte estipulado pelo governo
    resultado = classificar_status_saeb(743.0)
    assert resultado == 'Alfabetizado (Metas Saeb)'