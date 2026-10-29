-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Preparación del ambiente (Unity Catalog + ADLS con Managed Identity)
-- MAGIC Crea las ubicaciones externas (una por contenedor), el catálogo del entorno, los esquemas medallion y **todas las
-- MAGIC tablas como tablas EXTERNAS** con DDL explícito: si una tabla se elimina, los datos siguen en ADLS y la tabla se
-- MAGIC puede recrear. Es el único notebook en SQL; el ETL (bronze → silver → gold) está escrito en PySpark.
-- MAGIC
-- MAGIC **Conexión a ADLS:** Managed Identity (Access Connector con rol *Storage Blob Data Contributor*) → storage credential
-- MAGIC `cred_oracle_adls` → external locations. No se usan access keys, SAS ni montajes (ver `docs/conexion_adls.md`).
-- MAGIC
-- MAGIC Contenedores de la cuenta ADLS Gen2: `metastore` (ubicación administrada del catálogo), `raw` (insumos), `bronze`,
-- MAGIC `silver`, `gold` y `control`.

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'oracle_dev';
CREATE WIDGET TEXT cuenta DEFAULT 'storacleinvestment';

-- COMMAND ----------

-- DBTITLE 1,Ubicaciones externas (una por contenedor, todas con la misma credencial)
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_oracle_metastore URL 'abfss://metastore@${cuenta}.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL cred_oracle_adls);
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_oracle_raw       URL 'abfss://raw@${cuenta}.dfs.core.windows.net/'       WITH (STORAGE CREDENTIAL cred_oracle_adls) COMMENT 'Insumos crudos';
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_oracle_bronze    URL 'abfss://bronze@${cuenta}.dfs.core.windows.net/'    WITH (STORAGE CREDENTIAL cred_oracle_adls);
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_oracle_silver    URL 'abfss://silver@${cuenta}.dfs.core.windows.net/'    WITH (STORAGE CREDENTIAL cred_oracle_adls);
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_oracle_gold      URL 'abfss://gold@${cuenta}.dfs.core.windows.net/'      WITH (STORAGE CREDENTIAL cred_oracle_adls);
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_oracle_control   URL 'abfss://control@${cuenta}.dfs.core.windows.net/'   WITH (STORAGE CREDENTIAL cred_oracle_adls);

-- COMMAND ----------

-- DBTITLE 1,Catálogo y esquemas
CREATE CATALOG IF NOT EXISTS ${catalogo} MANAGED LOCATION 'abfss://metastore@${cuenta}.dfs.core.windows.net/${catalogo}'
  COMMENT 'Lakehouse de Oracle Investment';
CREATE SCHEMA IF NOT EXISTS ${catalogo}.bronze  COMMENT 'Copia fiel de los insumos con columnas de auditoría';
CREATE SCHEMA IF NOT EXISTS ${catalogo}.silver  COMMENT 'Datos limpios, tipados, deduplicados y enriquecidos';
CREATE SCHEMA IF NOT EXISTS ${catalogo}.gold    COMMENT 'KPIs y señales para consumo (BI, Genie, app, Delta Sharing)';
CREATE SCHEMA IF NOT EXISTS ${catalogo}.control COMMENT 'Bitácora de ejecuciones, calidad y solicitudes';

-- COMMAND ----------

-- DBTITLE 1,Bronze (externas)
CREATE TABLE IF NOT EXISTS ${catalogo}.bronze.precios (
  fecha STRING, ticker STRING, apertura STRING, maximo STRING, minimo STRING, cierre STRING, cierre_ajustado STRING, volumen STRING,
  _archivo_origen STRING, _archivo_modificado TIMESTAMP, _fecha_ingesta TIMESTAMP, _origen STRING)
USING DELTA LOCATION 'abfss://bronze@${cuenta}.dfs.core.windows.net/${catalogo}/precios';

CREATE TABLE IF NOT EXISTS ${catalogo}.bronze.dividendos (
  ticker STRING, fecha_pago STRING, dividendo DOUBLE,
  _archivo_origen STRING, _archivo_modificado TIMESTAMP, _fecha_ingesta TIMESTAMP, _origen STRING)
USING DELTA LOCATION 'abfss://bronze@${cuenta}.dfs.core.windows.net/${catalogo}/dividendos';

CREATE TABLE IF NOT EXISTS ${catalogo}.bronze.catalogo (
  ticker STRING, nombre STRING, tipo STRING, sector STRING, industria STRING, capitalizacion_usd STRING, moneda STRING,
  _archivo_origen STRING, _archivo_modificado TIMESTAMP, _fecha_ingesta TIMESTAMP, _origen STRING)
USING DELTA LOCATION 'abfss://bronze@${cuenta}.dfs.core.windows.net/${catalogo}/catalogo';

CREATE TABLE IF NOT EXISTS ${catalogo}.bronze.mapa_futuros (
  simbolo_futuro STRING, nombre_futuro STRING, ticker_subyacente STRING, valor_punto_usd STRING,
  _archivo_origen STRING, _archivo_modificado TIMESTAMP, _fecha_ingesta TIMESTAMP, _origen STRING)
USING DELTA LOCATION 'abfss://bronze@${cuenta}.dfs.core.windows.net/${catalogo}/mapa_futuros';

CREATE TABLE IF NOT EXISTS ${catalogo}.bronze.operaciones (
  id_operacion BIGINT, entrada_utc STRING, salida_utc STRING, fecha STRING, familia STRING, simbolo STRING, lado STRING, qty BIGINT,
  precio_entrada DOUBLE, precio_salida DOUBLE, puntos DOUBLE, usd_neto DOUBLE, mfe_ticks DOUBLE, mae_ticks DOUBLE,
  resultado STRING, duracion_min DOUBLE,
  _archivo_origen STRING, _archivo_modificado TIMESTAMP, _fecha_ingesta TIMESTAMP, _origen STRING)
USING DELTA LOCATION 'abfss://bronze@${cuenta}.dfs.core.windows.net/${catalogo}/operaciones';

-- COMMAND ----------

-- DBTITLE 1,Silver (externas)
CREATE TABLE IF NOT EXISTS ${catalogo}.silver.catalogo_activos (
  ticker STRING, nombre STRING, tipo STRING, sector STRING, industria STRING, capitalizacion_usd DOUBLE)
USING DELTA LOCATION 'abfss://silver@${cuenta}.dfs.core.windows.net/${catalogo}/catalogo_activos';

CREATE TABLE IF NOT EXISTS ${catalogo}.silver.precios_diarios (
  ticker STRING, fecha DATE, apertura DOUBLE, maximo DOUBLE, minimo DOUBLE, cierre DOUBLE, cierre_ajustado DOUBLE, volumen BIGINT,
  calidad_ohlc_ok BOOLEAN, rendimiento_diario DOUBLE, rendimiento_log DOUBLE, anio INT,
  nombre STRING, tipo STRING, sector STRING, industria STRING)
USING DELTA PARTITIONED BY (anio)
LOCATION 'abfss://silver@${cuenta}.dfs.core.windows.net/${catalogo}/precios_diarios';

CREATE TABLE IF NOT EXISTS ${catalogo}.silver.dividendos (ticker STRING, fecha_pago DATE, dividendo DOUBLE)
USING DELTA LOCATION 'abfss://silver@${cuenta}.dfs.core.windows.net/${catalogo}/dividendos';

CREATE TABLE IF NOT EXISTS ${catalogo}.silver.operaciones (
  fecha_mercado DATE, ticker_subyacente STRING, simbolo STRING, id_operacion BIGINT, entrada_utc TIMESTAMP, salida_utc TIMESTAMP,
  fecha DATE, familia STRING, lado STRING, contratos INT, precio_entrada DOUBLE, precio_salida DOUBLE, puntos DOUBLE,
  usd_neto DOUBLE, mfe_ticks DOUBLE, mae_ticks DOUBLE, resultado STRING, duracion_min DOUBLE, valor_punto_usd DOUBLE,
  gana BOOLEAN, con_subyacente BOOLEAN, rendimiento_subyacente DOUBLE, vix_cierre DOUBLE, regimen_vix STRING)
USING DELTA PARTITIONED BY (familia)
LOCATION 'abfss://silver@${cuenta}.dfs.core.windows.net/${catalogo}/operaciones';

-- COMMAND ----------

-- DBTITLE 1,Gold (externas)
CREATE TABLE IF NOT EXISTS ${catalogo}.gold.kpis_activo_anual (
  ticker STRING, anio INT, sesiones BIGINT, rendimiento_anual DOUBLE, volatilidad_anual DOUBLE, max_drawdown DOUBLE,
  cierre_fin_anio DOUBLE, dividendos_anio DOUBLE, pagos_anio BIGINT, rendimiento_dividendo DOUBLE, ratio_rend_riesgo DOUBLE)
USING DELTA LOCATION 'abfss://gold@${cuenta}.dfs.core.windows.net/${catalogo}/kpis_activo_anual';

CREATE TABLE IF NOT EXISTS ${catalogo}.gold.kpis_sector_anual (
  sector STRING, anio INT, activos BIGINT, rendimiento_promedio DOUBLE, volatilidad_promedio DOUBLE, peor_drawdown DOUBLE,
  rendimiento_dividendo_promedio DOUBLE)
USING DELTA LOCATION 'abfss://gold@${cuenta}.dfs.core.windows.net/${catalogo}/kpis_sector_anual';

CREATE TABLE IF NOT EXISTS ${catalogo}.gold.kpis_estrategias (
  familia STRING, mes STRING, regimen_vix STRING, operaciones BIGINT, tasa_acierto DOUBLE, ganancia_bruta DOUBLE,
  perdida_bruta DOUBLE, usd_neto DOUBLE, expectativa_usd DOUBLE, profit_factor DOUBLE)
USING DELTA LOCATION 'abfss://gold@${cuenta}.dfs.core.windows.net/${catalogo}/kpis_estrategias';

CREATE TABLE IF NOT EXISTS ${catalogo}.gold.senales_estrategias_base (
  ticker STRING, fecha DATE, cierre DOUBLE, sma200 DOUBLE, max55 DOUBLE, min55 DOUBLE, rsi2 DOUBLE, senal_e1 STRING, senal_e2 STRING)
USING DELTA LOCATION 'abfss://gold@${cuenta}.dfs.core.windows.net/${catalogo}/senales_estrategias_base';

CREATE TABLE IF NOT EXISTS ${catalogo}.gold.panel_activos (
  ticker STRING, fecha DATE, cierre DOUBLE, sma200 DOUBLE, max55 DOUBLE, min55 DOUBLE, rsi2 DOUBLE, senal_e1 STRING, senal_e2 STRING,
  nombre STRING, sector STRING, tipo STRING, rendimiento_anual DOUBLE, volatilidad_anual DOUBLE, max_drawdown DOUBLE)
USING DELTA LOCATION 'abfss://gold@${cuenta}.dfs.core.windows.net/${catalogo}/panel_activos';

-- COMMAND ----------

-- DBTITLE 1,Control (externas)
CREATE TABLE IF NOT EXISTS ${catalogo}.control.ejecuciones (
  fecha_utc TIMESTAMP, paso STRING, tabla STRING, filas BIGINT, estado STRING, detalle STRING)
USING DELTA LOCATION 'abfss://control@${cuenta}.dfs.core.windows.net/${catalogo}/ejecuciones';

CREATE TABLE IF NOT EXISTS ${catalogo}.control.calidad (
  fecha_utc TIMESTAMP, regla STRING, valor DOUBLE, umbral DOUBLE, critica BOOLEAN, cumple BOOLEAN)
USING DELTA LOCATION 'abfss://control@${cuenta}.dfs.core.windows.net/${catalogo}/calidad';

CREATE TABLE IF NOT EXISTS ${catalogo}.control.solicitudes_acceso (
  fecha_utc TIMESTAMP, correo STRING, tabla STRING, motivo STRING, estado STRING)
USING DELTA LOCATION 'abfss://control@${cuenta}.dfs.core.windows.net/${catalogo}/solicitudes_acceso';

-- COMMAND ----------

SHOW TABLES IN ${catalogo}.gold;
