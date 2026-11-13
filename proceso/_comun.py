# Databricks notebook source
# MAGIC %md
# MAGIC # Utilidades comunes (se ejecuta con `%run ./_comun`)
# MAGIC Widgets del entorno, acceso al paquete `oracle_lakehouse` del repositorio y registro de ejecuciones.

# COMMAND ----------

import os, sys, datetime as dt
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "oracle_dev", "Catálogo del entorno")
dbutils.widgets.text("ruta_raw", "abfss://raw@storacleinvestment.dfs.core.windows.net", "Ruta raw (ADLS)")
CATALOGO = dbutils.widgets.get("catalogo")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse (vacío = el primero disponible)")
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

def ejecutar_en_warehouse(sentencia: str):
    """Ejecuta una sentencia en el SQL warehouse y devuelve sus filas.
    El clúster dedicado del workflow no puede consultar ni alterar una tabla con filtro de fila o máscara de columna
    (ROW_COLUMN_ACCESS_POLICIES_NOT_SUPPORTED_ON_ASSIGNED_CLUSTERS); el SQL warehouse sí."""
    import time
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
    w = WorkspaceClient()
    warehouse = dbutils.widgets.get("warehouse_id") or next(iter(w.warehouses.list())).id
    r = w.statement_execution.execute_statement(warehouse_id=warehouse, statement=sentencia, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(5)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise Exception(f"{r.status.state}: {r.status.error.message if r.status.error else ''} · {sentencia[:120]}")
    return (r.result.data_array or []) if r.result else []

def suspender_proteccion(destino: str):
    """Quita el filtro de fila y las máscaras de columna de una tabla antes de reescribirla.
    La tarea final `grants`, que se ejecuta siempre (run_if ALL_DONE), vuelve a aplicar la protección aunque una
    tarea intermedia falle."""
    cat, esquema, nombre = destino.split(".")
    cond = f"table_schema = '{esquema}' AND table_name = '{nombre}'"
    if ejecutar_en_warehouse(f"SELECT 1 FROM {cat}.information_schema.row_filters WHERE {cond}"):
        ejecutar_en_warehouse(f"ALTER TABLE {destino} DROP ROW FILTER")
    for (columna,) in ejecutar_en_warehouse(f"SELECT column_name FROM {cat}.information_schema.column_masks WHERE {cond}"):
        ejecutar_en_warehouse(f"ALTER TABLE {destino} ALTER COLUMN {columna} DROP MASK")

def registrar_ejecucion(paso: str, tabla_destino: str, filas: int, estado: str = "OK", detalle: str = ""):
    """Bitácora de ejecuciones en control.ejecuciones (append)."""
    fila = [(dt.datetime.utcnow(), paso, tabla_destino, int(filas), estado, detalle)]
    escribir(spark.createDataFrame(fila, "fecha_utc timestamp, paso string, tabla string, filas long, estado string, detalle string"),
             tabla("control", "ejecuciones"))

print(f"Catálogo: {CATALOGO} · Raw: {RUTA_RAW}")
