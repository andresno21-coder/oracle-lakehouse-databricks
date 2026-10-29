-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Sharing · Compartir gold de forma segura
-- MAGIC Se comparte la capa gold (sin datos de operaciones individuales) con un destinatario externo.
-- MAGIC - **Databricks a Databricks:** el destinatario se identifica con el *sharing identifier* de su metastore.
-- MAGIC - **Abierto:** se genera un archivo de credenciales para leer con pandas, Power BI o Spark fuera de Databricks.

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'oracle_prd';

-- COMMAND ----------

CREATE SHARE IF NOT EXISTS oracle_kpis_mercado COMMENT 'KPIs de mercado y señales de Oracle Investment';

ALTER SHARE oracle_kpis_mercado ADD TABLE ${catalogo}.gold.kpis_activo_anual   WITH HISTORY;
ALTER SHARE oracle_kpis_mercado ADD TABLE ${catalogo}.gold.kpis_sector_anual   WITH HISTORY;
ALTER SHARE oracle_kpis_mercado ADD TABLE ${catalogo}.gold.panel_activos;

-- COMMAND ----------

-- Destinatario abierto (genera el enlace de activación con las credenciales)
CREATE RECIPIENT IF NOT EXISTS socio_research COMMENT 'Socio externo de research (lectura de KPIs)';
GRANT SELECT ON SHARE oracle_kpis_mercado TO RECIPIENT socio_research;

-- COMMAND ----------

DESCRIBE RECIPIENT socio_research;   -- copiar el activation_link y entregarlo por un canal seguro
SHOW ALL IN SHARE oracle_kpis_mercado;
