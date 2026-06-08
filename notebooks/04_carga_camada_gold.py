# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Carga da Camada Gold
# MAGIC
# MAGIC **Tech Challenge — Fase 2 | Arquitetura Medallion**
# MAGIC
# MAGIC **Autor:** Leonardo (Responsável PySpark — Camadas Silver e Gold)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## O que este notebook faz
# MAGIC
# MAGIC Este notebook lê os dados limpos da camada **Silver** e produz a camada **Gold**.
# MAGIC
# MAGIC A camada Gold contém **tabelas analíticas prontas para consumo** por:
# MAGIC - 📊 Dashboards (Streamlit / Power BI — responsabilidade do Reinaldo)
# MAGIC - 📈 Análises de negócio
# MAGIC - 🔍 Consultas SQL diretas
# MAGIC
# MAGIC ### Tabelas Gold produzidas
# MAGIC
# MAGIC | Tabela | Nível | Descrição |
# MAGIC |--------|-------|-----------|
# MAGIC | `gold.tc_fato_vendas_enriquecida` | Detalhe | Venda + dados do cliente (join) |
# MAGIC | `gold.tc_agg_vendas_por_categoria` | Agregado | Receita, volume e ticket médio por categoria |
# MAGIC | `gold.tc_agg_vendas_por_estado` | Agregado | Receita e performance por estado |
# MAGIC | `gold.tc_agg_vendas_por_canal` | Agregado | Comparativo Loja Física vs Online |
# MAGIC | `gold.tc_agg_vendas_por_segmento` | Agregado | Receita por segmento de cliente |
# MAGIC | `gold.tc_agg_tendencia_mensal` | Temporal | Evolução mensal de receita e volume |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pré-requisito
# MAGIC
# MAGIC Execute antes: `03_carga_camada_silver.py`

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Importações e verificação das dependências

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

# Tabelas de entrada (Silver)
silver_vendas   = "silver.tc_vendas_tratadas"
silver_clientes = "silver.tc_clientes_tratados"

# Verificação: as tabelas Silver precisam existir
for tabela in [silver_vendas, silver_clientes]:
    try:
        spark.read.table(tabela).limit(1).count()
    except Exception as erro:
        raise ValueError(
            f"Tabela '{tabela}' não encontrada. "
            f"Execute primeiro '03_carga_camada_silver.py'."
        ) from erro

print("✅ Tabelas Silver encontradas. Iniciando processamento Gold...")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Leitura da Silver

# COMMAND ----------
# Remove metadados de processamento da Silver — não pertencem à Gold
df_vendas   = spark.read.table(silver_vendas).drop("_data_processamento")
df_clientes = spark.read.table(silver_clientes).drop("_data_processamento")

print(f"Silver Vendas   → {df_vendas.count()} registros")
print(f"Silver Clientes → {df_clientes.count()} registros")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Gold 1 — Fato de Vendas Enriquecida (Detalhe)
# MAGIC
# MAGIC **Join** entre vendas e clientes para criar a tabela fato analítica completa.
# MAGIC
# MAGIC Usa `LEFT JOIN` para manter todas as vendas, mesmo que o cliente não esteja cadastrado.

# COMMAND ----------
df_fato_vendas = (
    df_vendas
    .join(df_clientes, on="id_cliente", how="left")

    # Adicionar campos temporais para facilitar agrupamentos no dashboard
    .withColumn("ano",  F.year("dt_venda"))
    .withColumn("mes",  F.month("dt_venda"))
    .withColumn("ano_mes", F.date_format("dt_venda", "yyyy-MM"))

    # Renomear colunas ambíguas (estado vem de vendas e clientes)
    .withColumnRenamed("estado", "estado_venda")
    .withColumn(
        "estado_cliente",
        F.col("estado")  # coluna de clientes (caso a join produza ambiguidade)
    )

    # Metadado Gold
    .withColumn("_data_processamento_gold", F.current_timestamp())
)

# Seleção final — ordem lógica das colunas para o dashboard
df_fato_vendas = df_fato_vendas.select(
    # Chaves
    "id_venda", "id_cliente",
    # Dimensões de tempo
    "dt_venda", "ano", "mes", "ano_mes",
    # Produto
    "produto", "categoria",
    # Valores
    "quantidade", "preco_unitario", "valor_total",
    # Canal e origem
    "canal", "origem", "cupom",
    # Status
    "status",
    # Dimensão cliente
    "nome", "email", "cidade", "estado_venda", "segmento", "data_cadastro",
    # Metadado
    "_data_processamento_gold"
)

print(f"✅ Fato Vendas Enriquecida → {df_fato_vendas.count()} registros")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Gold 2 — Agregação por Categoria
# MAGIC
# MAGIC **KPIs por categoria de produto:**
# MAGIC receita total, volume de vendas, ticket médio, quantidade total vendida.

# COMMAND ----------
df_agg_categoria = (
    df_fato_vendas
    .groupBy("categoria")
    .agg(
        F.count("id_venda")               .alias("total_vendas"),
        F.sum("valor_total")              .alias("receita_total"),
        F.round(F.avg("valor_total"), 2)  .alias("ticket_medio"),
        F.sum("quantidade")               .alias("quantidade_total"),
        F.round(F.avg("quantidade"), 2)   .alias("quantidade_media_por_venda"),
        F.countDistinct("id_cliente")     .alias("clientes_distintos")
    )
    .withColumn("receita_total", F.round("receita_total", 2))
    .withColumn("participacao_pct",
        F.round(
            F.col("receita_total") / F.sum("receita_total").over(Window.partitionBy()) * 100,
            2
        )
    )
    .orderBy(F.col("receita_total").desc())
)

print(f"✅ Agregado por Categoria → {df_agg_categoria.count()} categorias")
display(df_agg_categoria)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Gold 3 — Agregação por Estado
# MAGIC
# MAGIC **Performance geográfica:** receita e volume por estado de venda.

# COMMAND ----------
df_agg_estado = (
    df_fato_vendas
    .groupBy("estado_venda")
    .agg(
        F.count("id_venda")               .alias("total_vendas"),
        F.sum("valor_total")              .alias("receita_total"),
        F.round(F.avg("valor_total"), 2)  .alias("ticket_medio"),
        F.countDistinct("id_cliente")     .alias("clientes_distintos")
    )
    .withColumn("receita_total", F.round("receita_total", 2))
    .withColumn("participacao_pct",
        F.round(
            F.col("receita_total") / F.sum("receita_total").over(Window.partitionBy()) * 100,
            2
        )
    )
    .orderBy(F.col("receita_total").desc())
)

print(f"✅ Agregado por Estado → {df_agg_estado.count()} estados")
display(df_agg_estado)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Gold 4 — Agregação por Canal
# MAGIC
# MAGIC **Comparativo Loja Física vs Online.**

# COMMAND ----------
df_agg_canal = (
    df_fato_vendas
    .groupBy("canal")
    .agg(
        F.count("id_venda")               .alias("total_vendas"),
        F.sum("valor_total")              .alias("receita_total"),
        F.round(F.avg("valor_total"), 2)  .alias("ticket_medio"),
        F.sum("quantidade")               .alias("quantidade_total"),
        F.countDistinct("id_cliente")     .alias("clientes_distintos")
    )
    .withColumn("receita_total", F.round("receita_total", 2))
    .withColumn("participacao_pct",
        F.round(
            F.col("receita_total") / F.sum("receita_total").over(Window.partitionBy()) * 100,
            2
        )
    )
    .orderBy(F.col("receita_total").desc())
)

print(f"✅ Agregado por Canal → {df_agg_canal.count()} canais")
display(df_agg_canal)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Gold 5 — Agregação por Segmento de Cliente
# MAGIC
# MAGIC **Receita e comportamento por segmento de cliente.**

# COMMAND ----------
df_agg_segmento = (
    df_fato_vendas
    .filter(F.col("segmento").isNotNull())   # clientes sem segmento são excluídos
    .groupBy("segmento")
    .agg(
        F.count("id_venda")               .alias("total_vendas"),
        F.sum("valor_total")              .alias("receita_total"),
        F.round(F.avg("valor_total"), 2)  .alias("ticket_medio"),
        F.countDistinct("id_cliente")     .alias("clientes_distintos"),
        F.round(
            F.sum("valor_total") / F.countDistinct("id_cliente"), 2
        ).alias("receita_media_por_cliente")
    )
    .withColumn("receita_total", F.round("receita_total", 2))
    .orderBy(F.col("receita_total").desc())
)

print(f"✅ Agregado por Segmento → {df_agg_segmento.count()} segmentos")
display(df_agg_segmento)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Gold 6 — Tendência Mensal
# MAGIC
# MAGIC **Evolução mensal de receita e volume** — essencial para análises de tendência no dashboard.

# COMMAND ----------
df_tendencia_mensal = (
    df_fato_vendas
    .groupBy("ano", "mes", "ano_mes")
    .agg(
        F.count("id_venda")               .alias("total_vendas"),
        F.sum("valor_total")              .alias("receita_total"),
        F.round(F.avg("valor_total"), 2)  .alias("ticket_medio"),
        F.sum("quantidade")               .alias("quantidade_total"),
        F.countDistinct("id_cliente")     .alias("clientes_ativos")
    )
    .withColumn("receita_total", F.round("receita_total", 2))
    .orderBy("ano", "mes")
)

# Crescimento mês a mês (variação percentual)
janela_tempo = Window.orderBy("ano", "mes")
df_tendencia_mensal = df_tendencia_mensal.withColumn(
    "receita_mes_anterior",
    F.lag("receita_total").over(janela_tempo)
).withColumn(
    "crescimento_pct",
    F.round(
        (F.col("receita_total") - F.col("receita_mes_anterior"))
        / F.col("receita_mes_anterior") * 100,
        2
    )
)

print(f"✅ Tendência Mensal → {df_tendencia_mensal.count()} meses")
display(df_tendencia_mensal)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Gravação na camada Gold (Delta Lake)
# MAGIC
# MAGIC Todas as tabelas são gravadas em modo `overwrite` — garantindo idempotência.

# COMMAND ----------
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

tabelas_gold = {
    "gold.tc_fato_vendas_enriquecida": df_fato_vendas,
    "gold.tc_agg_vendas_por_categoria": df_agg_categoria,
    "gold.tc_agg_vendas_por_estado":    df_agg_estado,
    "gold.tc_agg_vendas_por_canal":     df_agg_canal,
    "gold.tc_agg_vendas_por_segmento":  df_agg_segmento,
    "gold.tc_agg_tendencia_mensal":     df_tendencia_mensal,
}

for nome_tabela, df in tabelas_gold.items():
    (
        df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(nome_tabela)
    )
    print(f"✅ {nome_tabela} → gravada ({df.count()} registros)")

print("\n🏆 Todas as tabelas Gold gravadas com sucesso!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Validação Final das tabelas Gold

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Visão geral da Fato Vendas Enriquecida
# MAGIC SELECT
# MAGIC   COUNT(*)                  AS total_registros,
# MAGIC   COUNT(DISTINCT id_venda)  AS vendas_unicas,
# MAGIC   COUNT(DISTINCT id_cliente) AS clientes_distintos,
# MAGIC   COUNT(DISTINCT categoria)  AS categorias,
# MAGIC   COUNT(DISTINCT estado_venda) AS estados,
# MAGIC   COUNT(DISTINCT canal)      AS canais,
# MAGIC   MIN(dt_venda)             AS periodo_inicio,
# MAGIC   MAX(dt_venda)             AS periodo_fim,
# MAGIC   ROUND(SUM(valor_total), 2) AS receita_total_geral
# MAGIC FROM gold.tc_fato_vendas_enriquecida

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Top 5 categorias por receita
# MAGIC SELECT categoria, total_vendas, receita_total, ticket_medio, participacao_pct
# MAGIC FROM gold.tc_agg_vendas_por_categoria
# MAGIC ORDER BY receita_total DESC
# MAGIC LIMIT 5

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Comparativo de canais
# MAGIC SELECT canal, total_vendas, receita_total, ticket_medio, participacao_pct
# MAGIC FROM gold.tc_agg_vendas_por_canal
# MAGIC ORDER BY receita_total DESC

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Evolução mensal de receita
# MAGIC SELECT ano_mes, total_vendas, receita_total, ticket_medio, crescimento_pct
# MAGIC FROM gold.tc_agg_tendencia_mensal
# MAGIC ORDER BY ano, mes

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Camada Gold concluída!
# MAGIC
# MAGIC ### Tabelas disponíveis para o Dashboard (Reinaldo)
# MAGIC
# MAGIC | Tabela | Uso no Dashboard |
# MAGIC |--------|-----------------|
# MAGIC | `gold.tc_fato_vendas_enriquecida` | Tabela base detalhada — filtros, drill-down |
# MAGIC | `gold.tc_agg_vendas_por_categoria` | Gráfico de barras / pizza por categoria |
# MAGIC | `gold.tc_agg_vendas_por_estado` | Mapa geográfico por estado |
# MAGIC | `gold.tc_agg_vendas_por_canal` | KPI comparativo Loja vs Online |
# MAGIC | `gold.tc_agg_vendas_por_segmento` | Análise de segmentação de clientes |
# MAGIC | `gold.tc_agg_tendencia_mensal` | Gráfico de linha — evolução temporal |
# MAGIC
# MAGIC ### Pipeline completo
# MAGIC
# MAGIC ```
# MAGIC origens → Bronze → Silver → Gold → Dashboard
# MAGIC   01          02      03      04      (Reinaldo)
# MAGIC ```
# MAGIC
# MAGIC ### Integração com NoSQL (Caio)
# MAGIC
# MAGIC As tabelas Gold podem ser exportadas para MongoDB/Redis/Cassandra
# MAGIC pelo notebook do Caio para persistência e consultas NoSQL.
