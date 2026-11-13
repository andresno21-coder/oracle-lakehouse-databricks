-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Reversión en SQL · Solo la parte lógica (Unity Catalog)
-- MAGIC Equivalente en SQL del paso 1 de `01_drop_medallion.py`. Las tablas son externas: después de este guion los
-- MAGIC archivos Delta siguen en ADLS; para borrarlos usar el notebook en Python (paso 2) o la celda indicada al final.

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'oracle_dev';

-- COMMAND ----------

-- DBTITLE 1,Seguridad de fila y de columna (hay que quitarla antes de eliminar la tabla)
ALTER TABLE ${catalogo}.gold.kpis_estrategias DROP ROW FILTER;
ALTER TABLE ${catalogo}.gold.kpis_estrategias ALTER COLUMN usd_neto DROP MASK;
DROP FUNCTION IF EXISTS ${catalogo}.gold.filtro_familias;
DROP FUNCTION IF EXISTS ${catalogo}.gold.mascara_usd;

-- COMMAND ----------

-- DBTITLE 1,Gold
DROP TABLE IF EXISTS ${catalogo}.gold.panel_activos;
DROP TABLE IF EXISTS ${catalogo}.gold.senales_estrategias_base;
DROP TABLE IF EXISTS ${catalogo}.gold.kpis_estrategias;
DROP TABLE IF EXISTS ${catalogo}.gold.kpis_sector_anual;
DROP TABLE IF EXISTS ${catalogo}.gold.kpis_activo_anual;

-- COMMAND ----------

-- DBTITLE 1,Silver
DROP TABLE IF EXISTS ${catalogo}.silver.operaciones;
DROP TABLE IF EXISTS ${catalogo}.silver.dividendos;
DROP TABLE IF EXISTS ${catalogo}.silver.precios_diarios;
DROP TABLE IF EXISTS ${catalogo}.silver.catalogo_activos;

-- COMMAND ----------

-- DBTITLE 1,Bronze
DROP TABLE IF EXISTS ${catalogo}.bronze.operaciones;
DROP TABLE IF EXISTS ${catalogo}.bronze.mapa_futuros;
DROP TABLE IF EXISTS ${catalogo}.bronze.catalogo;
DROP TABLE IF EXISTS ${catalogo}.bronze.dividendos;
DROP TABLE IF EXISTS ${catalogo}.bronze.precios;

-- COMMAND ----------

-- DBTITLE 1,Control
DROP TABLE IF EXISTS ${catalogo}.control.solicitudes_acceso;
DROP TABLE IF EXISTS ${catalogo}.control.calidad;
DROP TABLE IF EXISTS ${catalogo}.control.ejecuciones;

-- COMMAND ----------

-- DBTITLE 1,Esquemas (CASCADE elimina también los respaldos con fecha de control)
DROP SCHEMA IF EXISTS ${catalogo}.gold CASCADE;
DROP SCHEMA IF EXISTS ${catalogo}.silver CASCADE;
DROP SCHEMA IF EXISTS ${catalogo}.bronze CASCADE;
DROP SCHEMA IF EXISTS ${catalogo}.control CASCADE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Rutas físicas** (ejecutar en una celda de Python, una por capa):
-- MAGIC ```python
-- MAGIC for capa in ["gold", "silver", "bronze", "control"]:
-- MAGIC     dbutils.fs.rm(f"abfss://{capa}@storacleinvestment.dfs.core.windows.net/<catalogo>", recurse=True)
-- MAGIC ```
-- MAGIC **Catálogo y ubicaciones externas** (solo si se desmonta todo el proyecto):
-- MAGIC `DROP CATALOG IF EXISTS <catalogo> CASCADE;` y `DROP EXTERNAL LOCATION IF EXISTS ext_oracle_<contenedor>;`
