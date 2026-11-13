# Databricks notebook source
# MAGIC %md
# MAGIC # Orquestador manual (alternativa al workflow)
# MAGIC Ejecuta el pipeline completo, en el mismo orden que el workflow, desde un notebook. En producción se usa el **Job**
# MAGIC definido en `resources/job_oracle_etl.yml` (tareas en paralelo, reintentos y alertas), que GitHub Actions despliega
# MAGIC y ejecuta; este notebook sirve para pruebas en dev.

# COMMAND ----------

dbutils.widgets.text("catalogo", "oracle_dev")
dbutils.widgets.text("ruta_raw", "abfss://raw@storacleinvestment.dfs.core.windows.net")
p = {"catalogo": dbutils.widgets.get("catalogo"), "ruta_raw": dbutils.widgets.get("ruta_raw")}

from concurrent.futures import ThreadPoolExecutor
print(dbutils.notebook.run("1_preparacion_ambiente", 1800, {"catalogo": p["catalogo"]}))
fuentes = ["precios", "dividendos", "catalogo", "mapa_futuros", "operaciones"]
with ThreadPoolExecutor(max_workers=5) as ex:   # extract en paralelo
    list(ex.map(lambda f: dbutils.notebook.run("2_extract_bronze", 1800, {**p, "fuente": f}), fuentes))

for nb in ["3_transform_silver_mercado", "3_transform_silver_operaciones", "4_load_gold_kpis",
           "4_load_controles_calidad", "4_load_mantenimiento_delta", "4_load_base_transaccional"]:
    print(nb, dbutils.notebook.run(nb, 3600, p))
print(dbutils.notebook.run("5_grants", 1800, {"catalogo": p["catalogo"]}))
