# Databricks notebook source
# MAGIC %md
# MAGIC # Orquestador manual (alternativa al Job)
# MAGIC Ejecuta el pipeline completo en orden desde un notebook. En producción se usa el **Job** definido en
# MAGIC `resources/job_oracle_etl.yml` (tareas en paralelo, reintentos y alertas); este notebook sirve para pruebas en dev.

# COMMAND ----------

dbutils.widgets.text("catalogo", "oracle_dev")
dbutils.widgets.text("ruta_raw", "abfss://raw@storacleinvestment.dfs.core.windows.net")
p = {"catalogo": dbutils.widgets.get("catalogo"), "ruta_raw": dbutils.widgets.get("ruta_raw")}

from concurrent.futures import ThreadPoolExecutor
fuentes = ["precios", "dividendos", "catalogo", "mapa_futuros", "operaciones"]
with ThreadPoolExecutor(max_workers=5) as ex:   # bronze en paralelo
    list(ex.map(lambda f: dbutils.notebook.run("01_bronze/ingesta_bronze", 1800, {**p, "fuente": f}), fuentes))

for nb in ["02_silver/silver_mercado", "02_silver/silver_operaciones", "03_gold/gold_kpis", "04_calidad/controles_calidad"]:
    print(nb, dbutils.notebook.run(nb, 3600, p))
