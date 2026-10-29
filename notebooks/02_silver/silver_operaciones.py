# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver · Operaciones con contexto de mercado
# MAGIC Une tres fuentes: **operaciones** (Parquet) + **mapa de futuros** (CSV) + **precios del subyacente y VIX** (silver).
# MAGIC Cada operación queda con su ETF subyacente, el rendimiento del subyacente ese día y el régimen de volatilidad
# MAGIC (BAJO < 15 ≤ MEDIO ≤ 25 < ALTO). Las operaciones sin subyacente (cripto) se conservan y se marcan.

# COMMAND ----------

# MAGIC %run ../_comun

# COMMAND ----------

from delta.tables import DeltaTable

ops = tr.normalizar_operaciones(spark.table(tabla("bronze", "operaciones")), spark.table(tabla("bronze", "mapa_futuros")))
ops_ctx = tr.contexto_de_mercado(ops, spark.table(tabla("silver", "precios_diarios")))

destino = tabla("silver", "operaciones")
if not spark.catalog.tableExists(destino):
    ops_ctx.write.format("delta").partitionBy("familia").saveAsTable(destino)
else:
    (DeltaTable.forName(spark, destino).alias("t").merge(ops_ctx.alias("s"), "t.id_operacion = s.id_operacion")
               .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

n = ops_ctx.count()
sin_sub = ops_ctx.filter(~F.col("con_subyacente")).count()
registrar_ejecucion("silver_operaciones", destino, n, detalle=f"sin subyacente: {sin_sub}")
display(ops_ctx.groupBy("familia", "regimen_vix").count().orderBy("familia", "regimen_vix"))
