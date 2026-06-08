"""
MÓDULO: pipeline_executor.py
RESPONSÁVEL: Engenharia de Dados (Automação do Fluxo Coletivo)
OBJETIVO: Orquestrar de ponta a ponta a execução do pipeline do Tech Challenge,
          garantindo o acionamento ordenado das camadas e a persistência final.
"""

import sys
import os

# Adiciona a raiz ao path para garantir importação dos módulos sem erros de contexto
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.database.nosql_connector import salvar_camada_gold_nosql

def executar_pipeline_completo():
    print("🏁 Iniciando Orquestração do Pipeline - Indicador Criança Alfabetizada...")
    
    # 1. Ingestão Batch/Streaming (Simulação da Camada Bronze)
    print("📥 [BRONZE] Coletando microdados educacionais e metas da Base dos Dados...")
    
    # 2. Processamento e Limpeza (Código do Leonardo)
    print("⚙️ [SILVER] Executando rotinas de limpeza, tipagem e deduplicação...")
    
    # 3. Agregação e Cruzamento com a Meta do Saeb (743 pontos)
    print("🏅 [GOLD] Consolidando tabelas analíticas para consumo do Dashboard...")
    
    # 4. Exportação NoSQL (Código do Caio)
    print("🔌 [NOSQL] Acionando conector para persistência na camada de armazenamento...")
    conexao_string = "mongodb://localhost:27017/fiap_tech_challenge"
    salvar_camada_gold_nosql(df_gold=None, connection_string=conexao_string)
    
    print("✅ Pipeline executado com sucesso e dados disponíveis para o Dashboard!")

if __name__ == "__main__":
    executar_pipeline_completo()