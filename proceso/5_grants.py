# Databricks notebook source
# MAGIC %md
# MAGIC # 5 · Grants · Permisos por capa, filtro de filas y máscara de columna (Unity Catalog)
# MAGIC Última tarea del workflow. La carga de gold suspende el filtro y la máscara de `kpis_estrategias` para poder
# MAGIC reescribirla; aquí se reponen junto con los permisos. La tarea se ejecuta **siempre** (`run_if: ALL_DONE`), también
# MAGIC cuando una tarea anterior falla, para que gold nunca quede sin control de acceso. Es idempotente.
# MAGIC
# MAGIC | Grupo de cuenta | Rol | bronze | silver | gold | control |
# MAGIC |---|---|---|---|---|---|
# MAGIC | `oi_ingenieros_datos` | construyen y operan el ETL | ALL | ALL | ALL | ALL |
# MAGIC | `oi_analistas_riesgo` | investigan estrategias | — | SELECT | SELECT | SELECT |
# MAGIC | `oi_consumidores_bi` | tableros, Genie y app | — | — | SELECT | — |
# MAGIC | `oi_auditores` | revisan calidad y linaje | — | — | SELECT | SELECT |
# MAGIC
# MAGIC Los grupos son **de cuenta** (Unity Catalog no reconoce los grupos locales del workspace) y sus usuarios son
# MAGIC ficticios: ver `seguridad/usuarios_ficticios.md`. El mismo guion en SQL puro está en `seguridad/01_grupos_y_permisos.sql`
# MAGIC y Delta Sharing en `seguridad/02_delta_sharing.sql`.

# COMMAND ----------

dbutils.widgets.text("catalogo", "oracle_dev", "Catálogo del entorno")
CATALOGO = dbutils.widgets.get("catalogo")

INGENIEROS, ANALISTAS, CONSUMIDORES, AUDITORES = (
    "oi_ingenieros_datos", "oi_analistas_riesgo", "oi_consumidores_bi", "oi_auditores")

def otorgar(privilegios: str, objeto: str, grupo: str):
    spark.sql(f"GRANT {privilegios} ON {objeto} TO `{grupo}`")
    print(f"GRANT {privilegios} ON {objeto} TO {grupo}")

# COMMAND ----------

# DBTITLE 1,Catálogo y esquemas
otorgar("ALL PRIVILEGES", f"CATALOG {CATALOGO}", INGENIEROS)      # ingeniería: dueña del lakehouse del entorno
for grupo in (ANALISTAS, CONSUMIDORES, AUDITORES):
    otorgar("USE CATALOG", f"CATALOG {CATALOGO}", grupo)

LECTURA_POR_GRUPO = {
    ANALISTAS: ["silver", "gold", "control"],     # nada de bronze
    CONSUMIDORES: ["gold"],                       # solo la capa de consumo
    AUDITORES: ["gold", "control"],               # KPIs y bitácoras
}
for grupo, esquemas in LECTURA_POR_GRUPO.items():
    for esquema in esquemas:
        otorgar("USE SCHEMA, SELECT", f"SCHEMA {CATALOGO}.{esquema}", grupo)

# COMMAND ----------

# DBTITLE 1,Ubicaciones externas: solo ingeniería lee raw y escribe en las capas
otorgar("READ FILES", "EXTERNAL LOCATION ext_oracle_raw", INGENIEROS)
for capa in ("bronze", "silver", "gold", "control"):
    otorgar("READ FILES, WRITE FILES, CREATE EXTERNAL TABLE", f"EXTERNAL LOCATION ext_oracle_{capa}", INGENIEROS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seguridad a nivel de fila y de columna en gold
# MAGIC Los consumidores de BI ven solo la familia de estrategias con evidencia (F7) y no ven el USD neto.

# COMMAND ----------

kpis = f"{CATALOGO}.gold.kpis_estrategias"

spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOGO}.gold.filtro_familias(familia STRING)
  RETURN is_account_group_member('{INGENIEROS}') OR is_account_group_member('{ANALISTAS}') OR familia = 'F7'""")
spark.sql(f"ALTER TABLE {kpis} SET ROW FILTER {CATALOGO}.gold.filtro_familias ON (familia)")

spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOGO}.gold.mascara_usd(valor DOUBLE)
  RETURN CASE WHEN is_account_group_member('{CONSUMIDORES}') THEN NULL ELSE valor END""")
spark.sql(f"ALTER TABLE {kpis} ALTER COLUMN usd_neto SET MASK {CATALOGO}.gold.mascara_usd")

# COMMAND ----------

# DBTITLE 1,Evidencia: permisos efectivos sobre gold
permisos = spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOGO}.gold")
display(permisos)
dbutils.notebook.exit(f"{permisos.count()} permisos vigentes en {CATALOGO}.gold")
