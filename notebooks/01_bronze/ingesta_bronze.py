# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze · Ingesta incremental desde ADLS (PySpark)
# MAGIC Lee una fuente de la capa **raw** en ADLS y la agrega a su tabla Delta de bronze.
# MAGIC
# MAGIC - **Un notebook, cinco fuentes:** el parámetro `fuente` elige qué leer; el Job lanza las cinco tareas en paralelo.
# MAGIC - **Esquema explícito** por fuente (no se infiere): un cambio en el archivo de origen falla de forma visible.
# MAGIC - **Incremental e idempotente:** se registra la ruta y la fecha de modificación de cada archivo (`_metadata`);
# MAGIC   un archivo ya ingerido no se vuelve a cargar aunque el Job se repita.

# COMMAND ----------

# MAGIC %run ../_comun

# COMMAND ----------

from pyspark.sql import types as T

dbutils.widgets.dropdown("fuente", "precios", ["precios", "dividendos", "catalogo", "mapa_futuros", "operaciones"], "Fuente")
FUENTE = dbutils.widgets.get("fuente")

ESQUEMAS = {
    "precios": T.StructType([T.StructField(c, T.StringType()) for c in
                             ["fecha", "ticker", "apertura", "maximo", "minimo", "cierre", "cierre_ajustado", "volumen"]]),
    "dividendos": T.StructType([T.StructField("ticker", T.StringType()), T.StructField("fecha_pago", T.StringType()),
                                T.StructField("dividendo", T.DoubleType())]),
    "catalogo": T.StructType([T.StructField(c, T.StringType()) for c in
                              ["ticker", "nombre", "tipo", "sector", "industria", "capitalizacion_usd", "moneda"]]),
    "mapa_futuros": T.StructType([T.StructField(c, T.StringType()) for c in
                                  ["simbolo_futuro", "nombre_futuro", "ticker_subyacente", "valor_punto_usd"]]),
}
ORIGEN = {  # ruta relativa dentro del contenedor raw y formato
    "precios": ("precios/", "csv"), "dividendos": ("dividendos/", "json"),
    "catalogo": ("catalogo/catalogo_activos.csv", "csv"), "mapa_futuros": ("catalogo/mapa_futuros.csv", "csv"),
    "operaciones": ("operaciones/", "parquet"),
}

# COMMAND ----------

ruta, formato = ORIGEN[FUENTE]
lector = spark.read.format(formato)
if formato == "csv":
    lector = lector.option("header", True)
if FUENTE in ESQUEMAS:
    lector = lector.schema(ESQUEMAS[FUENTE])
df = (lector.load(f"{RUTA_RAW}/{ruta}")
            .withColumn("_archivo_origen", F.col("_metadata.file_path"))
            .withColumn("_archivo_modificado", F.col("_metadata.file_modification_time")))
df = tr.agregar_auditoria(df, FUENTE)

destino = tabla("bronze", FUENTE)
if spark.catalog.tableExists(destino):
    ya = spark.table(destino).select("_archivo_origen", "_archivo_modificado").distinct()
    df = df.join(ya, ["_archivo_origen", "_archivo_modificado"], "left_anti")

filas = df.count()
if filas:
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(destino)
registrar_ejecucion(f"bronze_{FUENTE}", destino, filas, detalle=f"{RUTA_RAW}/{ruta}")
print(f"{destino}: {filas} filas nuevas")
