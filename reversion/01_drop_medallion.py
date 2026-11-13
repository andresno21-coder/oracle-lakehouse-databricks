# Databricks notebook source
# MAGIC %md
# MAGIC # Reversión · Elimina las tablas lógicas y las rutas físicas del lakehouse
# MAGIC Deja el entorno como antes del despliegue. Como las tablas son **externas**, `DROP TABLE` solo borra la definición
# MAGIC en Unity Catalog: los archivos Delta siguen en ADLS y hay que borrarlos aparte. Por eso la reversión tiene dos pasos:
# MAGIC
# MAGIC 1. **Lógico:** quita filtro y máscara, elimina tablas, funciones, esquemas y (opcional) el catálogo.
# MAGIC 2. **Físico:** borra la carpeta del catálogo en los contenedores `bronze`, `silver`, `gold` y `control`.
# MAGIC
# MAGIC El contenedor `raw` (insumos) **no se toca**: con él y el notebook `proceso/1_preparacion_ambiente` todo se reconstruye.
# MAGIC
# MAGIC Protección: en dev no hace nada si `confirmar` no es exactamente `ELIMINAR`; en cualquier otro catálogo hay que
# MAGIC escribir además su nombre (`ELIMINAR oracle_prd`), así nadie revierte producción por dejar valores por defecto.

# COMMAND ----------

dbutils.widgets.text("catalogo", "oracle_dev", "Catálogo a revertir")
dbutils.widgets.text("cuenta", "storacleinvestment", "Cuenta ADLS Gen2")
dbutils.widgets.text("confirmar", "", "ELIMINAR (dev) o ELIMINAR <catálogo>")
dbutils.widgets.dropdown("borrar_fisico", "si", ["si", "no"], "Borrar también las rutas físicas")
dbutils.widgets.dropdown("borrar_catalogo", "no", ["si", "no"], "Eliminar el catálogo y las ubicaciones externas")
CATALOGO = dbutils.widgets.get("catalogo")
CUENTA = dbutils.widgets.get("cuenta")
CAPAS = ["gold", "silver", "bronze", "control"]      # en orden inverso a como se construyen

CLAVE = "ELIMINAR" if CATALOGO.endswith("_dev") else f"ELIMINAR {CATALOGO}"
if dbutils.widgets.get("confirmar").strip() != CLAVE:
    dbutils.notebook.exit(f"Sin cambios: el parámetro `confirmar` debe ser exactamente: {CLAVE}")

# COMMAND ----------

# DBTITLE 1,Inventario previo (queda en la salida del notebook como registro de lo eliminado)
inventario = (spark.table(f"{CATALOGO}.information_schema.tables")
                   .filter(f"table_schema IN ({', '.join(repr(c) for c in CAPAS)})")
                   .select("table_schema", "table_name", "table_type"))
display(inventario.orderBy("table_schema", "table_name"))
tablas = [(r.table_schema, r.table_name) for r in inventario.collect()]

# COMMAND ----------

# DBTITLE 1,Paso 1 · Reversión lógica (Unity Catalog)
kpis = f"{CATALOGO}.gold.kpis_estrategias"
if ("gold", "kpis_estrategias") in tablas:           # la tabla no se puede eliminar mientras tenga filtro o máscara
    spark.sql(f"ALTER TABLE {kpis} DROP ROW FILTER")
    spark.sql(f"ALTER TABLE {kpis} ALTER COLUMN usd_neto DROP MASK")
for funcion in ("filtro_familias", "mascara_usd"):
    spark.sql(f"DROP FUNCTION IF EXISTS {CATALOGO}.gold.{funcion}")

for capa, nombre in tablas:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOGO}.{capa}.{nombre}")
    print(f"DROP TABLE {CATALOGO}.{capa}.{nombre}")
for capa in CAPAS:
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOGO}.{capa} CASCADE")
    print(f"DROP SCHEMA {CATALOGO}.{capa}")

# COMMAND ----------

# DBTITLE 1,Paso 2 · Reversión física (archivos Delta en ADLS)
if dbutils.widgets.get("borrar_fisico") == "si":
    for capa in CAPAS:
        carpeta = f"abfss://{capa}@{CUENTA}.dfs.core.windows.net/{CATALOGO}"
        try:
            dbutils.fs.rm(carpeta, recurse=True)
            print(f"Eliminada {carpeta}")
        except Exception as e:                       # la carpeta puede no existir si la capa nunca se cargó
            print(f"Sin cambios en {carpeta}: {str(e)[:120]}")

# COMMAND ----------

# DBTITLE 1,Paso 3 (opcional) · Catálogo y ubicaciones externas
if dbutils.widgets.get("borrar_catalogo") == "si":
    spark.sql(f"DROP CATALOG IF EXISTS {CATALOGO} CASCADE")
    otros = [r.catalog for r in spark.sql("SHOW CATALOGS").collect() if r.catalog.startswith("oracle_")]
    if not otros:                                    # las ubicaciones se comparten entre dev y prod
        for c in ["metastore", "raw", "bronze", "silver", "gold", "control"]:
            spark.sql(f"DROP EXTERNAL LOCATION IF EXISTS ext_oracle_{c}")
    else:
        print(f"Se conservan las ubicaciones externas: las usa {otros}")

dbutils.notebook.exit(f"Reversión de {CATALOGO} terminada: {len(tablas)} tablas eliminadas")
