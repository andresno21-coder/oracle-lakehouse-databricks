# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver · Mercado (precios, dividendos y catálogo)
# MAGIC - Tipado, limpieza y deduplicación de precios (una fila por ticker y fecha; la ingesta más reciente gana).
# MAGIC - Rendimientos diarios simple y logarítmico, y **join** con el catálogo (sector, industria, tipo).
# MAGIC - Carga con **MERGE** (upsert) usando la API de Delta Lake en PySpark: solo cambia lo nuevo o corregido.

# COMMAND ----------

# MAGIC %run ../_comun

# COMMAND ----------

from delta.tables import DeltaTable

def upsert(df, destino: str, claves: list):
    """MERGE por claves; si la tabla no existe se crea. Devuelve las filas del origen."""
    if not spark.catalog.tableExists(destino):
        df.write.format("delta").saveAsTable(destino)
    else:
        cond = " AND ".join(f"t.{c} = s.{c}" for c in claves)
        (DeltaTable.forName(spark, destino).alias("t").merge(df.alias("s"), cond)
                   .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    return df.count()

# COMMAND ----------

catalogo = tr.normalizar_catalogo(spark.table(tabla("bronze", "catalogo")))
n_cat = upsert(catalogo, tabla("silver", "catalogo_activos"), ["ticker"])

precios = tr.enriquecer_con_catalogo(
    tr.agregar_rendimientos(tr.normalizar_precios(spark.table(tabla("bronze", "precios")))), catalogo)
n_pre = upsert(precios, tabla("silver", "precios_diarios"), ["ticker", "fecha"])

dividendos = tr.normalizar_dividendos(spark.table(tabla("bronze", "dividendos")))
n_div = upsert(dividendos, tabla("silver", "dividendos"), ["ticker", "fecha_pago"])

for t, n in [("catalogo_activos", n_cat), ("precios_diarios", n_pre), ("dividendos", n_div)]:
    registrar_ejecucion(f"silver_{t}", tabla("silver", t), n)
print(n_cat, n_pre, n_div)

# COMMAND ----------

# Optimización de lectura para las consultas por ticker y fecha
DeltaTable.forName(spark, tabla("silver", "precios_diarios")).optimize().executeZOrderBy("ticker", "fecha")
