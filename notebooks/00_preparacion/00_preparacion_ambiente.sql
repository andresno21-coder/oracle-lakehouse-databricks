-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Preparación del ambiente (Unity Catalog + ADLS)
-- MAGIC Crea el catálogo del entorno, las ubicaciones externas sobre ADLS Gen2 y los esquemas de la arquitectura medallion.
-- MAGIC Es el único notebook en SQL: el ETL (bronze → silver → gold) está escrito en PySpark.
-- MAGIC
-- MAGIC **Requisitos previos (una vez, en Azure):** cuenta de almacenamiento ADLS Gen2 con los contenedores `raw` y `lakehouse`,
-- MAGIC un *Access Connector for Azure Databricks* con el rol **Storage Blob Data Contributor** sobre la cuenta, y la
-- MAGIC *storage credential* `cred_oracle_adls` creada en Unity Catalog con ese conector.

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'oracle_dev';
CREATE WIDGET TEXT cuenta_almacenamiento DEFAULT 'storacleinvestment';

-- COMMAND ----------

-- Ubicaciones externas: los insumos (raw) viven fuera de Databricks, en ADLS; nunca en un Volume
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_oracle_raw
  URL CONCAT('abfss://raw@', '${cuenta_almacenamiento}', '.dfs.core.windows.net/')
  WITH (STORAGE CREDENTIAL cred_oracle_adls)
  COMMENT 'Insumos crudos de Oracle Investment (precios, dividendos, catálogo, operaciones)';

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_oracle_lakehouse
  URL CONCAT('abfss://lakehouse@', '${cuenta_almacenamiento}', '.dfs.core.windows.net/')
  WITH (STORAGE CREDENTIAL cred_oracle_adls)
  COMMENT 'Almacenamiento administrado del lakehouse (tablas Delta de bronze, silver y gold)';

-- COMMAND ----------

-- Un catálogo por entorno (dev / prod): esquemas y tablas se llaman igual en ambos
CREATE CATALOG IF NOT EXISTS ${catalogo}
  MANAGED LOCATION CONCAT('abfss://lakehouse@', '${cuenta_almacenamiento}', '.dfs.core.windows.net/', '${catalogo}')
  COMMENT 'Lakehouse de Oracle Investment';

CREATE SCHEMA IF NOT EXISTS ${catalogo}.bronze COMMENT 'Copia fiel de los insumos, con columnas de auditoría';
CREATE SCHEMA IF NOT EXISTS ${catalogo}.silver COMMENT 'Datos limpios, tipados, deduplicados y enriquecidos';
CREATE SCHEMA IF NOT EXISTS ${catalogo}.gold   COMMENT 'KPIs y señales listos para consumo (BI, Genie, app, Delta Sharing)';
CREATE SCHEMA IF NOT EXISTS ${catalogo}.control COMMENT 'Bitácora de ejecuciones y controles de calidad';

-- COMMAND ----------

SHOW SCHEMAS IN ${catalogo};
