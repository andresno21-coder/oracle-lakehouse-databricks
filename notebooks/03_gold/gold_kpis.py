# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold · KPIs y señales
# MAGIC | Tabla | Grano | Para qué |
# MAGIC |---|---|---|
# MAGIC | `kpis_activo_anual` | ticker × año | rendimiento, volatilidad, máximo drawdown, rendimiento por dividendo, rendimiento/riesgo |
# MAGIC | `kpis_sector_anual` | sector × año | comparación sectorial |
# MAGIC | `kpis_estrategias` | familia × mes × régimen VIX | operaciones, acierto, profit factor, expectativa USD |
# MAGIC | `senales_estrategias_base` | ticker × día | señales E1 (tendencia) y E2 (reversión) |

# COMMAND ----------

# MAGIC %run ../_comun

# COMMAND ----------

precios = spark.table(tabla("silver", "precios_diarios"))
dividendos = spark.table(tabla("silver", "dividendos"))
catalogo = spark.table(tabla("silver", "catalogo_activos"))
operaciones = spark.table(tabla("silver", "operaciones"))

kpi_activo = tr.kpis_activo_anual(precios, dividendos)
salidas = {
    "kpis_activo_anual": kpi_activo,
    "kpis_sector_anual": tr.kpis_sector_anual(kpi_activo, catalogo),
    "kpis_estrategias": tr.kpis_estrategias(operaciones),
    "senales_estrategias_base": tr.senales_estrategias_base(precios),
}
for nombre, df in salidas.items():
    destino = tabla("gold", nombre)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino)
    registrar_ejecucion(f"gold_{nombre}", destino, spark.table(destino).count())

# COMMAND ----------

# Vista de consumo para BI / Genie: última señal por activo con su KPI del año en curso
spark.table(tabla("gold", "senales_estrategias_base")).createOrReplaceTempView("senales")
ultima = (spark.table(tabla("gold", "senales_estrategias_base"))
               .groupBy("ticker").agg(F.max("fecha").alias("fecha")))
vista = (spark.table(tabla("gold", "senales_estrategias_base")).join(ultima, ["ticker", "fecha"])
              .join(catalogo.select("ticker", "nombre", "sector", "tipo"), "ticker", "left")
              .join(kpi_activo.filter(F.col("anio") == F.year(F.current_date())).select("ticker", "rendimiento_anual", "volatilidad_anual", "max_drawdown"), "ticker", "left"))
vista.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla("gold", "panel_activos"))
display(vista.orderBy("ticker"))
