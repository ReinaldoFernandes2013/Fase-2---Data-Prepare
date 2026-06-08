# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Carga da Camada Silver
# MAGIC
# MAGIC **Tech Challenge — Fase 2 | Arquitetura Medallion**
# MAGIC
# MAGIC **Autor:** Leonardo (Responsável PySpark — Camadas Silver e Gold)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## O que este notebook faz
# MAGIC
# MAGIC Este notebook lê os dados brutos da camada **Bronze** e produz a camada **Silver**.
# MAGIC
# MAGIC A camada Silver aplica:
# MAGIC - ✅ **Limpeza**: remoção de registros inválidos e nulos críticos
# MAGIC - ✅ **Padronização**: nomes de colunas, formatos de texto e datas
# MAGIC - ✅ **Tipagem**: conversão para tipos corretos (string → date, string → double)
# MAGIC - ✅ **Enriquecimento**: campos calculados como `valor_total`
# MAGIC - ✅ **Deduplicação**: remoção de registros duplicados
# MAGIC - ✅ **Rastreabilidade**: metadado `_data_processamento`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pré-requisito
# MAGIC
# MAGIC Execute antes: `01_criacao_origens_transacionais.py` e `02_carga_camada_bronze.py`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Saída — tabelas criadas no schema `silver`
# MAGIC
# MAGIC | Tabela | Descrição |
# MAGIC |--------|-----------|
# MAGIC | `silver.tc_vendas_tratadas` | Vendas limpas, tipadas e com campos calculados |
# MAGIC | `silver.tc_clientes_tratados` | Clientes limpos e padronizados |

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Importações e verificação das dependências

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

# Tabelas de entrada (Bronze)
bronze_vendas   = "bronze.aula3_vendas_raw"
bronze_clientes = "bronze.aula3_clientes_raw"

# Verificação: as tabelas Bronze precisam existir
for tabela in [bronze_vendas, bronze_clientes]:
    try:
        spark.read.table(tabela).limit(1).count()
    except Exception as erro:
        raise ValueError(
            f"Tabela '{tabela}' não encontrada. "
            f"Execute primeiro '02_carga_camada_bronze.py'."
        ) from erro

print("✅ Tabelas Bronze encontradas. Iniciando processamento Silver...")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Leitura da Bronze

# COMMAND ----------
# Leitura removendo metadados de ingestão da Bronze (não pertencem à Silver)
df_vendas_raw   = spark.read.table(bronze_vendas).drop("_data_ingestao", "_fonte")
df_clientes_raw = spark.read.table(bronze_clientes).drop("_data_ingestao", "_fonte")

print(f"Bronze Vendas   → {df_vendas_raw.count()} registros, {len(df_vendas_raw.columns)} colunas")
print(f"Bronze Clientes → {df_clientes_raw.count()} registros, {len(df_clientes_raw.columns)} colunas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Silver — Processamento de Vendas
# MAGIC
# MAGIC ### 3.1 Seleção do contrato de colunas
# MAGIC
# MAGIC Definimos explicitamente quais colunas fazem parte do contrato Silver.
# MAGIC Colunas extras ou temporárias da Bronze são descartadas aqui.

# COMMAND ----------
df_base_vendas = df_vendas_raw.select(
    "id_venda",
    "data_venda",
    "produto",
    "categoria",
    "quantidade",
    "preco_unitario",
    "estado",
    "id_cliente",
    "cupom",
    "origem",
    "status_bruto"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.2 Limpeza e Tipagem
# MAGIC
# MAGIC | Transformação | Motivo |
# MAGIC |---------------|--------|
# MAGIC | `data_venda` → `dt_venda` (DateType) | Padronização do nome + tipo correto para filtros de data |
# MAGIC | `valor_total` calculado | Campo analítico derivado de quantidade × preço |
# MAGIC | `canal` derivado de `origem` | Simplificação para análise de canal de venda |
# MAGIC | `status` normalizado | Padronização de texto (maiúsculo, sem espaços extras) |
# MAGIC | Remoção de nulos críticos | Registros sem `id_venda` ou `id_cliente` são inválidos |
# MAGIC | Deduplicação por `id_venda` | Garante unicidade do identificador de venda |

# COMMAND ----------
df_vendas_tratadas = (
    df_base_vendas

    # ── Renomear e converter data ──────────────────────────────────────────
    .withColumnRenamed("data_venda", "dt_venda")
    .withColumn("dt_venda", F.to_date(F.col("dt_venda"), "yyyy-MM-dd"))

    # ── Campo calculado: valor total da venda ──────────────────────────────
    .withColumn(
        "valor_total",
        (F.col("quantidade").cast(T.IntegerType()) *
         F.col("preco_unitario").cast(T.DoubleType()))
    )

    # ── Tipagem correta ────────────────────────────────────────────────────
    .withColumn("quantidade",      F.col("quantidade").cast(T.IntegerType()))
    .withColumn("preco_unitario",  F.col("preco_unitario").cast(T.DoubleType()))
    .withColumn("id_venda",        F.col("id_venda").cast(T.IntegerType()))
    .withColumn("id_cliente",      F.col("id_cliente").cast(T.IntegerType()))

    # ── Padronização de texto ──────────────────────────────────────────────
    .withColumn("produto",    F.initcap(F.trim(F.col("produto"))))
    .withColumn("categoria",  F.initcap(F.trim(F.col("categoria"))))
    .withColumn("estado",     F.upper(F.trim(F.col("estado"))))
    .withColumn("origem",     F.trim(F.col("origem")))

    # ── Derivar campo "canal" a partir de "origem" ─────────────────────────
    # Regra: se origem começa com "Loja" → "Loja Física", senão → "Online"
    .withColumn(
        "canal",
        F.when(F.col("origem").startswith("Loja"), F.lit("Loja Física"))
         .otherwise(F.lit("Online"))
    )

    # ── Normalizar status ──────────────────────────────────────────────────
    .withColumnRenamed("status_bruto", "status")
    .withColumn("status", F.upper(F.trim(F.col("status"))))

    # ── Cupom: null → "SEM_CUPOM" ──────────────────────────────────────────
    .withColumn("cupom", F.coalesce(F.col("cupom"), F.lit("SEM_CUPOM")))

    # ── Metadado de processamento Silver ──────────────────────────────────
    .withColumn("_data_processamento", F.current_timestamp())
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.3 Remoção de registros inválidos (nulos críticos)

# COMMAND ----------
antes = df_vendas_tratadas.count()

df_vendas_tratadas = df_vendas_tratadas.filter(
    F.col("id_venda").isNotNull() &
    F.col("id_cliente").isNotNull() &
    F.col("dt_venda").isNotNull() &
    F.col("valor_total").isNotNull() &
    (F.col("valor_total") > 0)
)

depois = df_vendas_tratadas.count()
print(f"🗑️  Registros removidos (nulos/inválidos): {antes - depois}")
print(f"✅ Registros válidos de Vendas: {depois}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.4 Deduplicação de Vendas
# MAGIC
# MAGIC Mantemos apenas o registro mais recente por `id_venda`,
# MAGIC caso existam duplicatas vindas de múltiplas origens na Bronze.

# COMMAND ----------
janela_dedup = Window.partitionBy("id_venda").orderBy(F.col("_data_processamento").desc())

df_vendas_tratadas = (
    df_vendas_tratadas
    .withColumn("_rank", F.row_number().over(janela_dedup))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)

print(f"✅ Após deduplicação — Vendas únicas: {df_vendas_tratadas.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Silver — Processamento de Clientes
# MAGIC
# MAGIC ### 4.1 Seleção do contrato de colunas

# COMMAND ----------
df_base_clientes = df_clientes_raw.select(
    "id_cliente",
    "nome",
    "email",
    "cidade",
    "estado",
    "data_cadastro",
    "segmento"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.2 Limpeza e Tipagem de Clientes

# COMMAND ----------
df_clientes_tratados = (
    df_base_clientes

    # ── Tipagem ────────────────────────────────────────────────────────────
    .withColumn("id_cliente",    F.col("id_cliente").cast(T.IntegerType()))
    .withColumn("data_cadastro", F.to_date(F.col("data_cadastro"), "yyyy-MM-dd"))

    # ── Padronização de texto ──────────────────────────────────────────────
    .withColumn("nome",      F.initcap(F.trim(F.col("nome"))))
    .withColumn("email",     F.lower(F.trim(F.col("email"))))
    .withColumn("cidade",    F.initcap(F.trim(F.col("cidade"))))
    .withColumn("estado",    F.upper(F.trim(F.col("estado"))))
    .withColumn("segmento",  F.initcap(F.trim(F.col("segmento"))))

    # ── Metadado de processamento Silver ──────────────────────────────────
    .withColumn("_data_processamento", F.current_timestamp())
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.3 Remoção de registros inválidos de Clientes

# COMMAND ----------
antes = df_clientes_tratados.count()

df_clientes_tratados = df_clientes_tratados.filter(
    F.col("id_cliente").isNotNull() &
    F.col("nome").isNotNull() &
    F.col("email").isNotNull()
)

depois = df_clientes_tratados.count()
print(f"🗑️  Registros removidos (nulos críticos): {antes - depois}")
print(f"✅ Registros válidos de Clientes: {depois}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.4 Deduplicação de Clientes
# MAGIC
# MAGIC Mantemos apenas o registro mais recente por `id_cliente`.

# COMMAND ----------
janela_dedup_cli = Window.partitionBy("id_cliente").orderBy(F.col("data_cadastro").desc())

df_clientes_tratados = (
    df_clientes_tratados
    .withColumn("_rank", F.row_number().over(janela_dedup_cli))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)

print(f"✅ Após deduplicação — Clientes únicos: {df_clientes_tratados.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Gravação na camada Silver (Delta Lake)
# MAGIC
# MAGIC Criamos o schema `silver` se não existir e gravamos as tabelas em formato Delta.
# MAGIC O modo `overwrite` garante idempotência — pode ser re-executado sem duplicar dados.

# COMMAND ----------
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# Gravar Vendas Silver
(
    df_vendas_tratadas
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.tc_vendas_tratadas")
)

# Gravar Clientes Silver
(
    df_clientes_tratados
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.tc_clientes_tratados")
)

print("✅ Tabelas Silver gravadas com sucesso!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Validação das tabelas Silver

# COMMAND ----------
# MAGIC %md
# MAGIC ### Validação: silver.tc_vendas_tratadas

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)              AS total_registros,
# MAGIC   COUNT(DISTINCT id_venda)   AS vendas_unicas,
# MAGIC   COUNT(DISTINCT id_cliente) AS clientes_distintos,
# MAGIC   MIN(dt_venda)         AS data_mais_antiga,
# MAGIC   MAX(dt_venda)         AS data_mais_recente,
# MAGIC   ROUND(SUM(valor_total), 2) AS receita_total,
# MAGIC   COUNT(DISTINCT canal) AS canais_distintos,
# MAGIC   COUNT(DISTINCT status) AS status_distintos
# MAGIC FROM silver.tc_vendas_tratadas

# COMMAND ----------
# MAGIC %md
# MAGIC ### Validação: silver.tc_clientes_tratados

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                    AS total_registros,
# MAGIC   COUNT(DISTINCT id_cliente)  AS clientes_unicos,
# MAGIC   COUNT(DISTINCT estado)      AS estados_distintos,
# MAGIC   COUNT(DISTINCT segmento)    AS segmentos_distintos,
# MAGIC   MIN(data_cadastro)          AS cadastro_mais_antigo,
# MAGIC   MAX(data_cadastro)          AS cadastro_mais_recente
# MAGIC FROM silver.tc_clientes_tratados

# COMMAND ----------
# MAGIC %md
# MAGIC ### Amostra: Vendas Silver

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.tc_vendas_tratadas
# MAGIC LIMIT 10

# COMMAND ----------
# MAGIC %md
# MAGIC ### Amostra: Clientes Silver

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.tc_clientes_tratados
# MAGIC LIMIT 10

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Camada Silver concluída!
# MAGIC
# MAGIC ### Próximo passo
# MAGIC Execute: `04_carga_camada_gold.py`
# MAGIC
# MAGIC ### Resumo das transformações aplicadas
# MAGIC
# MAGIC | Processo | Vendas | Clientes |
# MAGIC |----------|--------|----------|
# MAGIC | Tipagem correta | ✅ | ✅ |
# MAGIC | Padronização de texto | ✅ | ✅ |
# MAGIC | Remoção de nulos críticos | ✅ | ✅ |
# MAGIC | Deduplicação | ✅ por `id_venda` | ✅ por `id_cliente` |
# MAGIC | Campo calculado `valor_total` | ✅ | — |
# MAGIC | Campo derivado `canal` | ✅ | — |
# MAGIC | Metadado `_data_processamento` | ✅ | ✅ |
