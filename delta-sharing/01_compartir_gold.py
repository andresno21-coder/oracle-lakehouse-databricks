# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Sharing · Compartir la capa gold con un destinatario externo
# MAGIC Publica los KPIs de mercado (sin operaciones individuales) en el share `oracle_kpis_mercado` y da acceso de
# MAGIC lectura al destinatario abierto `socio_research`, que los consume fuera de Databricks con `02_leer_share.py`.
# MAGIC
# MAGIC Requisitos: ser *metastore admin* (o tener `CREATE SHARE` y `CREATE RECIPIENT`) y que el metastore permita
# MAGIC compartir hacia afuera (`INTERNAL_AND_EXTERNAL`). Es idempotente. El mismo guion en SQL puro está en
# MAGIC `seguridad/02_delta_sharing.sql`.

# COMMAND ----------

dbutils.widgets.text("catalogo", "oracle_prd", "Catálogo del entorno")
dbutils.widgets.text("share", "oracle_kpis_mercado", "Nombre del share")
dbutils.widgets.text("destinatario", "socio_research", "Destinatario (recipient)")
CATALOGO = dbutils.widgets.get("catalogo")
SHARE = dbutils.widgets.get("share")
DESTINATARIO = dbutils.widgets.get("destinatario")

# Tabla de gold → ¿se comparte con historial? (permite time travel y lectura incremental al destinatario)
TABLAS = {
    "kpis_activo_anual": True,
    "kpis_sector_anual": True,
    "panel_activos": False,
}

# COMMAND ----------

# DBTITLE 1,Share y tablas compartidas
spark.sql(f"CREATE SHARE IF NOT EXISTS {SHARE} COMMENT 'KPIs de mercado y señales de Oracle Investment'")

ya_compartidas = {fila["name"] for fila in spark.sql(f"SHOW ALL IN SHARE {SHARE}").collect()}
for nombre, con_historial in TABLAS.items():
    if f"gold.{nombre}" in ya_compartidas:
        print(f"Ya estaba en el share: gold.{nombre}")
        continue
    historial = "WITH HISTORY" if con_historial else ""
    spark.sql(f"ALTER SHARE {SHARE} ADD TABLE {CATALOGO}.gold.{nombre} {historial}")
    print(f"Agregada al share: gold.{nombre} {historial}")

# COMMAND ----------

# DBTITLE 1,Destinatario abierto y permiso de lectura
spark.sql(f"CREATE RECIPIENT IF NOT EXISTS {DESTINATARIO} COMMENT 'Socio externo de research (lectura de KPIs)'")
spark.sql(f"GRANT SELECT ON SHARE {SHARE} TO RECIPIENT {DESTINATARIO}")

# COMMAND ----------

# DBTITLE 1,Evidencia: contenido del share y permisos
# El enlace de activación del destinatario es un secreto: se consulta con DESCRIBE RECIPIENT y se entrega por un
# canal seguro. Aquí no se muestra.
contenido = spark.sql(f"SHOW ALL IN SHARE {SHARE}")
display(contenido)
display(spark.sql(f"SHOW GRANTS ON SHARE {SHARE}"))
assert contenido.count() == len(TABLAS), f"Se esperaban {len(TABLAS)} tablas en el share"
dbutils.notebook.exit(f"{contenido.count()} tablas compartidas en {SHARE} con {DESTINATARIO}")
