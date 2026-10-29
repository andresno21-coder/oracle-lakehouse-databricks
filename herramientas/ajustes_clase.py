# -*- coding: utf-8 -*-
"""Aplica los ajustes de alineación con lo enseñado en clase (tablas externas, Job, permisos, orquestador)."""
from pathlib import Path
R = Path(__file__).resolve().parents[1]

def edit(rel, reps):
    p = R / rel; s = p.read_text(encoding="utf-8")
    for a, b in reps:
        assert a in s, (rel, a[:70]); s = s.replace(a, b)
    p.write_text(s, encoding="utf-8")

edit("notebooks/_comun.py", [
    ('''def registrar_ejecucion(paso: str, tabla_destino: str, filas: int, estado: str = "OK", detalle: str = ""):
    """Bitácora de ejecuciones en control.ejecuciones (append)."""
    fila = [(dt.datetime.utcnow(), paso, tabla_destino, int(filas), estado, detalle)]
    (spark.createDataFrame(fila, "fecha_utc timestamp, paso string, tabla string, filas long, estado string, detalle string")
          .write.mode("append").saveAsTable(tabla("control", "ejecuciones")))''',
     '''def escribir(df, destino: str, sobrescribir: bool = False):
    """Escribe en una tabla EXTERNA ya creada por 00_preparacion_ambiente (alinea las columnas por nombre).
    No usa saveAsTable: así la tabla nunca se recrea como administrada y conserva su LOCATION en ADLS."""
    columnas = spark.table(destino).columns
    df.select(*columnas).write.insertInto(destino, overwrite=sobrescribir)

def registrar_ejecucion(paso: str, tabla_destino: str, filas: int, estado: str = "OK", detalle: str = ""):
    """Bitácora de ejecuciones en control.ejecuciones (append)."""
    fila = [(dt.datetime.utcnow(), paso, tabla_destino, int(filas), estado, detalle)]
    escribir(spark.createDataFrame(fila, "fecha_utc timestamp, paso string, tabla string, filas long, estado string, detalle string"),
             tabla("control", "ejecuciones"))'''),
    ('import os, sys, datetime as dt\nfrom pyspark.sql import functions as F\n',
     'import os, sys, datetime as dt\nfrom pyspark.sql import functions as F\n\ndbutils.widgets.removeAll() if False else None  # los widgets se definen abajo; el Job los sobrescribe por parámetro\n'),
])

edit("notebooks/01_bronze/ingesta_bronze.py", [
    ('''destino = tabla("bronze", FUENTE)
if spark.catalog.tableExists(destino):
    ya = spark.table(destino).select("_archivo_origen", "_archivo_modificado").distinct()
    df = df.join(ya, ["_archivo_origen", "_archivo_modificado"], "left_anti")

filas = df.count()
if filas:
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(destino)''',
     '''destino = tabla("bronze", FUENTE)   # tabla externa creada por 00_preparacion_ambiente
ya = spark.table(destino).select("_archivo_origen", "_archivo_modificado").distinct()
df = df.join(ya, ["_archivo_origen", "_archivo_modificado"], "left_anti")

filas = df.count()
if filas:
    escribir(df, destino)''')])

edit("notebooks/02_silver/silver_mercado.py", [
    ('''    """MERGE por claves; si la tabla no existe se crea. Devuelve las filas del origen."""
    if not spark.catalog.tableExists(destino):
        df.write.format("delta").saveAsTable(destino)
    else:''', '''    """MERGE por claves sobre la tabla externa; si está vacía, inserción directa. Devuelve las filas del origen."""
    if spark.table(destino).isEmpty():
        escribir(df, destino)
    else:''')])

edit("notebooks/02_silver/silver_operaciones.py", [
    ('''if not spark.catalog.tableExists(destino):
    ops_ctx.write.format("delta").partitionBy("familia").saveAsTable(destino)
else:''', '''if spark.table(destino).isEmpty():          # tabla externa particionada por familia (00_preparacion_ambiente)
    escribir(ops_ctx, destino)
else:''')])

edit("notebooks/03_gold/gold_kpis.py", [
    ('''    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino)
    registrar_ejecucion''', '''    escribir(df, destino, sobrescribir=True)
    registrar_ejecucion'''),
    ('''# Vista de consumo para BI / Genie: última señal por activo con su KPI del año en curso
spark.table(tabla("gold", "senales_estrategias_base")).createOrReplaceTempView("senales")
''', '''# Vista de consumo para BI / Genie: última señal por activo con su KPI del año en curso
'''),
    ('''vista.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla("gold", "panel_activos"))''',
     '''escribir(vista, tabla("gold", "panel_activos"), sobrescribir=True)'''),
])

edit("notebooks/04_calidad/controles_calidad.py", [
    ('res.write.mode("append").saveAsTable(tabla("control", "calidad"))', 'escribir(res, tabla("control", "calidad"))')])

edit("notebooks/orquestador.py", [
    ('''from concurrent.futures import ThreadPoolExecutor''', '''from concurrent.futures import ThreadPoolExecutor
dbutils.notebook.run("00_preparacion/00_preparacion_ambiente", 1800, {"catalogo": p["catalogo"]})'''),
    ('''for nb in ["02_silver/silver_mercado", "02_silver/silver_operaciones", "03_gold/gold_kpis", "04_calidad/controles_calidad"]:''',
     '''for nb in ["02_silver/silver_mercado", "02_silver/silver_operaciones", "03_gold/gold_kpis",
           "04_calidad/controles_calidad", "04_calidad/mantenimiento_delta", "05_servir/exportar_base_transaccional"]:'''),
])

job = "resources/job_oracle_etl.yml"
s = (R / job).read_text(encoding="utf-8")
s = s.replace("          max_retries: 2\n", "          max_retries: 2\n          min_retry_interval_millis: 120000   # espera 2 min: los reintentos solo sirven ante fallas de plataforma o insumos\n")
s = s.replace("      email_notifications:\n        on_failure: [ \"${var.correo_alertas}\" ]",
              "      email_notifications:\n        on_start: [ \"${var.correo_alertas}\" ]\n        on_success: [ \"${var.correo_alertas}\" ]\n        on_failure: [ \"${var.correo_alertas}\" ]")
for t in ("silver_mercado", "silver_operaciones", "gold_kpis", "controles_calidad", "exportar_base_transaccional"):
    s = s.replace(f"        - task_key: {t}\n", f"        - task_key: {t}\n          run_if: ALL_SUCCESS\n")
s = s.replace('''        - task_key: exportar_base_transaccional
          run_if: ALL_SUCCESS
          depends_on: [ { task_key: controles_calidad } ]''', '''        - task_key: mantenimiento_delta
          run_if: ALL_SUCCESS
          depends_on: [ { task_key: controles_calidad } ]
          job_cluster_key: etl
          notebook_task: { notebook_path: ../notebooks/04_calidad/mantenimiento_delta.py }

        - task_key: exportar_base_transaccional
          run_if: ALL_SUCCESS
          depends_on: [ { task_key: controles_calidad } ]''')
s = s.replace("          notebook_task:\n            notebook_path: ../notebooks/00_preparacion/00_preparacion_ambiente.sql",
              "          notebook_task:\n            notebook_path: ../notebooks/00_preparacion/00_preparacion_ambiente.sql\n            base_parameters: { cuenta: storacleinvestment }")
(R / job).write_text(s, encoding="utf-8")

edit("seguridad/01_grupos_y_permisos.sql", [
    ('''-- Las ubicaciones externas solo las usa ingeniería
GRANT READ FILES ON EXTERNAL LOCATION ext_oracle_raw TO `oi_ingenieros_datos`;''',
     '''-- Ubicaciones externas: solo ingeniería lee los insumos y escribe las tablas externas de cada capa
GRANT READ FILES ON EXTERNAL LOCATION ext_oracle_raw TO `oi_ingenieros_datos`;
GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE ON EXTERNAL LOCATION ext_oracle_bronze  TO `oi_ingenieros_datos`;
GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE ON EXTERNAL LOCATION ext_oracle_silver  TO `oi_ingenieros_datos`;
GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE ON EXTERNAL LOCATION ext_oracle_gold    TO `oi_ingenieros_datos`;
GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE ON EXTERNAL LOCATION ext_oracle_control TO `oi_ingenieros_datos`;
-- Evidencia: Catalog Explorer → External data → cada ubicación → "Test connection" en verde (guardar captura)''')])
print("ajustes aplicados")
