# Databricks notebook source
# MAGIC %md
# MAGIC # 1 · Preparación del ambiente (Unity Catalog + ADLS con Managed Identity)
# MAGIC Primera tarea del workflow. Deja listo el entorno donde corre el ETL:
# MAGIC
# MAGIC 1. **Ubicaciones externas**, una por contenedor de ADLS Gen2, todas con la credencial `cred_oracle_adls`
# MAGIC    (Managed Identity del Access Connector; sin access keys, SAS ni montajes).
# MAGIC 2. **Catálogo** del entorno y **esquemas** medallion (`bronze`, `silver`, `gold`, `control`).
# MAGIC 3. **Tablas EXTERNAS** con DDL explícito: si una tabla se elimina, los datos siguen en ADLS.
# MAGIC
# MAGIC Es idempotente (`IF NOT EXISTS`): se ejecuta en cada corrida sin efectos secundarios. El mismo DDL está en SQL
# MAGIC puro en `PrepAmb/01_preparacion_ambiente.sql`; la reversión está en `reversion/`.

# COMMAND ----------

dbutils.widgets.text("catalogo", "oracle_dev", "Catálogo del entorno")
dbutils.widgets.text("cuenta", "storacleinvestment", "Cuenta ADLS Gen2")
dbutils.widgets.text("credencial", "cred_oracle_adls", "Storage credential (Managed Identity)")
CATALOGO = dbutils.widgets.get("catalogo")
CUENTA = dbutils.widgets.get("cuenta")
CREDENCIAL = dbutils.widgets.get("credencial")

def ruta(contenedor: str, *partes: str) -> str:
    return f"abfss://{contenedor}@{CUENTA}.dfs.core.windows.net/" + "/".join(partes)

# COMMAND ----------

# DBTITLE 1,Ubicaciones externas (una por contenedor)
CONTENEDORES = ["metastore", "raw", "bronze", "silver", "gold", "control"]
for c in CONTENEDORES:
    spark.sql(f"CREATE EXTERNAL LOCATION IF NOT EXISTS ext_oracle_{c} URL '{ruta(c)}' "
              f"WITH (STORAGE CREDENTIAL {CREDENCIAL})")
display(spark.sql("SHOW EXTERNAL LOCATIONS").filter("name LIKE 'ext_oracle_%'").select("name", "url"))

# COMMAND ----------

# DBTITLE 1,Catálogo y esquemas
ESQUEMAS = {
    "bronze": "Copia fiel de los insumos con columnas de auditoría",
    "silver": "Datos limpios, tipados, deduplicados y enriquecidos",
    "gold": "KPIs y señales para consumo (BI, Genie, app, Delta Sharing)",
    "control": "Bitácora de ejecuciones, calidad y solicitudes",
}
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO} MANAGED LOCATION '{ruta('metastore', CATALOGO)}' "
          "COMMENT 'Lakehouse de Oracle Investment'")
for esquema, comentario in ESQUEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{esquema} COMMENT '{comentario}'")

# COMMAND ----------

# DBTITLE 1,Definición de las tablas: (capa, nombre, columnas, partición)
AUDITORIA = "_archivo_origen STRING, _archivo_modificado TIMESTAMP, _fecha_ingesta TIMESTAMP, _origen STRING"
TABLAS = [
    ("bronze", "precios",
     "fecha STRING, ticker STRING, apertura STRING, maximo STRING, minimo STRING, cierre STRING, "
     "cierre_ajustado STRING, volumen STRING, " + AUDITORIA,
     None),
    ("bronze", "dividendos",
     "ticker STRING, fecha_pago STRING, dividendo DOUBLE, " + AUDITORIA,
     None),
    ("bronze", "catalogo",
     "ticker STRING, nombre STRING, tipo STRING, sector STRING, industria STRING, "
     "capitalizacion_usd STRING, moneda STRING, " + AUDITORIA,
     None),
    ("bronze", "mapa_futuros",
     "simbolo_futuro STRING, nombre_futuro STRING, ticker_subyacente STRING, valor_punto_usd STRING, " + AUDITORIA,
     None),
    ("bronze", "operaciones",
     "id_operacion BIGINT, entrada_utc STRING, salida_utc STRING, fecha STRING, familia STRING, "
     "simbolo STRING, lado STRING, qty BIGINT, precio_entrada DOUBLE, precio_salida DOUBLE, puntos DOUBLE, "
     "usd_neto DOUBLE, mfe_ticks DOUBLE, mae_ticks DOUBLE, resultado STRING, duracion_min DOUBLE, " + AUDITORIA,
     None),
    ("silver", "catalogo_activos",
     "ticker STRING, nombre STRING, tipo STRING, sector STRING, industria STRING, "
     "capitalizacion_usd DOUBLE",
     None),
    ("silver", "precios_diarios",
     "ticker STRING, fecha DATE, apertura DOUBLE, maximo DOUBLE, minimo DOUBLE, cierre DOUBLE, "
     "cierre_ajustado DOUBLE, volumen BIGINT, calidad_ohlc_ok BOOLEAN, rendimiento_diario DOUBLE, "
     "rendimiento_log DOUBLE, anio INT, nombre STRING, tipo STRING, sector STRING, industria STRING",
     'anio'),
    ("silver", "dividendos",
     "ticker STRING, fecha_pago DATE, dividendo DOUBLE",
     None),
    ("silver", "operaciones",
     "fecha_mercado DATE, ticker_subyacente STRING, simbolo STRING, id_operacion BIGINT, "
     "entrada_utc TIMESTAMP, salida_utc TIMESTAMP, fecha DATE, familia STRING, lado STRING, contratos INT, "
     "precio_entrada DOUBLE, precio_salida DOUBLE, puntos DOUBLE, usd_neto DOUBLE, mfe_ticks DOUBLE, "
     "mae_ticks DOUBLE, resultado STRING, duracion_min DOUBLE, valor_punto_usd DOUBLE, gana BOOLEAN, "
     "con_subyacente BOOLEAN, rendimiento_subyacente DOUBLE, vix_cierre DOUBLE, regimen_vix STRING",
     'familia'),
    ("gold", "kpis_activo_anual",
     "ticker STRING, anio INT, sesiones BIGINT, rendimiento_anual DOUBLE, volatilidad_anual DOUBLE, "
     "max_drawdown DOUBLE, cierre_fin_anio DOUBLE, dividendos_anio DOUBLE, pagos_anio BIGINT, "
     "rendimiento_dividendo DOUBLE, ratio_rend_riesgo DOUBLE",
     None),
    ("gold", "kpis_sector_anual",
     "sector STRING, anio INT, activos BIGINT, rendimiento_promedio DOUBLE, volatilidad_promedio DOUBLE, "
     "peor_drawdown DOUBLE, rendimiento_dividendo_promedio DOUBLE",
     None),
    ("gold", "kpis_estrategias",
     "familia STRING, mes STRING, regimen_vix STRING, operaciones BIGINT, tasa_acierto DOUBLE, "
     "ganancia_bruta DOUBLE, perdida_bruta DOUBLE, usd_neto DOUBLE, expectativa_usd DOUBLE, "
     "profit_factor DOUBLE",
     None),
    ("gold", "senales_estrategias_base",
     "ticker STRING, fecha DATE, cierre DOUBLE, sma200 DOUBLE, max55 DOUBLE, min55 DOUBLE, rsi2 DOUBLE, "
     "senal_e1 STRING, senal_e2 STRING",
     None),
    ("gold", "panel_activos",
     "ticker STRING, fecha DATE, cierre DOUBLE, sma200 DOUBLE, max55 DOUBLE, min55 DOUBLE, rsi2 DOUBLE, "
     "senal_e1 STRING, senal_e2 STRING, nombre STRING, sector STRING, tipo STRING, "
     "rendimiento_anual DOUBLE, volatilidad_anual DOUBLE, max_drawdown DOUBLE",
     None),
    ("control", "ejecuciones",
     "fecha_utc TIMESTAMP, paso STRING, tabla STRING, filas BIGINT, estado STRING, detalle STRING",
     None),
    ("control", "calidad",
     "fecha_utc TIMESTAMP, regla STRING, valor DOUBLE, umbral DOUBLE, critica BOOLEAN, cumple BOOLEAN",
     None),
    ("control", "solicitudes_acceso",
     "fecha_utc TIMESTAMP, correo STRING, tabla STRING, motivo STRING, estado STRING",
     None),
]

# COMMAND ----------

# DBTITLE 1,Creación de las tablas externas
for capa, nombre, columnas, particion in TABLAS:
    particionado = f"PARTITIONED BY ({particion})" if particion else ""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOGO}.{capa}.{nombre} ({columnas}) "
              f"USING DELTA {particionado} LOCATION '{ruta(capa, CATALOGO, nombre)}'")
    print(f"OK  {CATALOGO}.{capa}.{nombre}")

# COMMAND ----------

# DBTITLE 1,Verificación: ninguna tabla puede quedar administrada
tablas = (spark.table(f"{CATALOGO}.information_schema.tables")
               .filter("table_schema IN ('bronze', 'silver', 'gold', 'control')")
               .select("table_schema", "table_name", "table_type"))
display(tablas.orderBy("table_schema", "table_name"))
administradas = tablas.filter("table_type = 'MANAGED'").count()
assert administradas == 0, f"Hay {administradas} tablas administradas; todas deben ser externas"
dbutils.notebook.exit(f"{len(TABLAS)} tablas externas listas en {CATALOGO}")
