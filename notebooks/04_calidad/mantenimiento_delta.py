# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Mantenimiento y auditoría de tablas Delta
# MAGIC - **Historial y detalle** de las tablas silver (`DESCRIBE HISTORY` / `DESCRIBE DETAIL` vía API de Delta).
# MAGIC - **Time travel:** compara la versión actual de `precios_diarios` con la anterior (filas nuevas o corregidas).
# MAGIC - **OPTIMIZE + ZORDER** en gold y **VACUUM** con la retención por defecto (7 días): nunca `RETAIN 0` en producción,
# MAGIC   porque borra el historial y deja sin efecto el time travel.
# MAGIC - **Respaldo:** `SHALLOW CLONE` de la tabla gold de estrategias antes de cada sobrescritura (solo metadatos).

# COMMAND ----------

# MAGIC %run ../_comun

# COMMAND ----------

from delta.tables import DeltaTable

dt_precios = DeltaTable.forName(spark, tabla("silver", "precios_diarios"))
historial = dt_precios.history(10).select("version", "timestamp", "operation", "operationMetrics")
display(historial)
display(dt_precios.detail().select("format", "location", "numFiles", "sizeInBytes", "partitionColumns"))

# COMMAND ----------

# Time travel: qué cambió entre la versión anterior y la actual
versiones = [r.version for r in historial.orderBy(F.desc("version")).limit(2).collect()]
if len(versiones) == 2:
    actual, anterior = versiones
    previo = spark.read.option("versionAsOf", anterior).table(tabla("silver", "precios_diarios"))
    cambios = spark.table(tabla("silver", "precios_diarios")).exceptAll(previo)
    print(f"Versión {anterior} → {actual}: {cambios.count()} filas nuevas o corregidas")
    display(cambios.groupBy("ticker").count().orderBy(F.desc("count")))

# COMMAND ----------

for t in ["kpis_activo_anual", "senales_estrategias_base"]:
    DeltaTable.forName(spark, tabla("gold", t)).optimize().executeZOrderBy("ticker")

for capa, t in [("silver", "precios_diarios"), ("silver", "operaciones"), ("gold", "kpis_activo_anual")]:
    DeltaTable.forName(spark, tabla(capa, t)).vacuum()          # retención por defecto: 168 horas

# COMMAND ----------

# Respaldo de la tabla gold de estrategias (clon superficial con fecha)
sufijo = dt.datetime.utcnow().strftime("%Y%m%d")
spark.sql(f"CREATE OR REPLACE TABLE {tabla('control', 'respaldo_kpis_estrategias_' + sufijo)} "
          f"SHALLOW CLONE {tabla('gold', 'kpis_estrategias')}")
registrar_ejecucion("mantenimiento_delta", tabla("silver", "precios_diarios"), 0, detalle=f"versiones {versiones}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Recuperación ante un error de carga (solo en dev, como demostración):**
# MAGIC `DeltaTable.forName(spark, tabla("silver", "precios_diarios")).restoreToVersion(<versión_buena>)`
