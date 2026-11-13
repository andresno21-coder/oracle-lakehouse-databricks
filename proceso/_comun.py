# Databricks notebook source
# MAGIC %md
# MAGIC # Utilidades comunes (se ejecuta con `%run ./_comun` o `%run ./_comun`)
# MAGIC Widgets del entorno, acceso al paquete `oracle_lakehouse` del repositorio y registro de ejecuciones.

# COMMAND ----------

import os, sys, datetime as dt
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "oracle_dev", "Catálogo del entorno")
dbutils.widgets.text("ruta_raw", "abfss://raw@storacleinvestment.dfs.core.windows.net", "Ruta raw (ADLS)")
CATALOGO = dbutils.widgets.get("catalogo")
RUTA_RAW = dbutils.widgets.get("ruta_raw").rstrip("/")

# El paquete oracle_lakehouse vive junto a los notebooks (carpeta proceso/), así producción solo necesita esta carpeta
if os.getcwd() not in sys.path:
    sys.path.insert(0, os.getcwd())
from oracle_lakehouse import transformaciones as tr

def tabla(capa: str, nombre: str) -> str:
    return f"{CATALOGO}.{capa}.{nombre}"

def escribir(df, destino: str, sobrescribir: bool = False):
    """Escribe en una tabla EXTERNA ya creada por 1_preparacion_ambiente (alinea las columnas por nombre).
    No usa saveAsTable: así la tabla nunca se recrea como administrada y conserva su LOCATION en ADLS."""
    columnas = spark.table(destino).columns
    df.select(*columnas).write.insertInto(destino, overwrite=sobrescribir)

def registrar_ejecucion(paso: str, tabla_destino: str, filas: int, estado: str = "OK", detalle: str = ""):
    """Bitácora de ejecuciones en control.ejecuciones (append)."""
    fila = [(dt.datetime.utcnow(), paso, tabla_destino, int(filas), estado, detalle)]
    escribir(spark.createDataFrame(fila, "fecha_utc timestamp, paso string, tabla string, filas long, estado string, detalle string"),
             tabla("control", "ejecuciones"))

print(f"Catálogo: {CATALOGO} · Raw: {RUTA_RAW}")
