-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Seguridad · Grupos, usuarios ficticios y permisos por capa (Unity Catalog)
-- MAGIC Los grupos y usuarios se crean en la **consola de cuenta** (accounts.azuredatabricks.net → User management)
-- MAGIC y se asignan al workspace. Los usuarios del proyecto son ficticios (ver `usuarios_ficticios.md`).
-- MAGIC
-- MAGIC | Grupo | Rol | bronze | silver | gold | control |
-- MAGIC |---|---|---|---|---|---|
-- MAGIC | `oi_ingenieros_datos` | construyen y operan el ETL | ALL | ALL | ALL | ALL |
-- MAGIC | `oi_analistas_riesgo` | investigan estrategias | — | SELECT | SELECT | SELECT |
-- MAGIC | `oi_consumidores_bi` | tableros, Genie y app | — | — | SELECT | — |
-- MAGIC | `oi_auditores` | revisan calidad y linaje | — | — | SELECT | SELECT |

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'oracle_dev';

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG ${catalogo} TO `oi_ingenieros_datos`, `oi_analistas_riesgo`, `oi_consumidores_bi`, `oi_auditores`;

-- Ingeniería: dueña de todo el lakehouse del entorno
GRANT ALL PRIVILEGES ON CATALOG ${catalogo} TO `oi_ingenieros_datos`;

-- Analistas de riesgo: lectura de silver y gold, nada de bronze
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalogo}.silver  TO `oi_analistas_riesgo`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalogo}.gold    TO `oi_analistas_riesgo`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalogo}.control TO `oi_analistas_riesgo`;

-- Consumidores de BI: solo gold
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalogo}.gold TO `oi_consumidores_bi`;

-- Auditores: gold y bitácoras de control
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalogo}.gold    TO `oi_auditores`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalogo}.control TO `oi_auditores`;

-- Ubicaciones externas: solo ingeniería lee los insumos y escribe las tablas externas de cada capa
GRANT READ FILES ON EXTERNAL LOCATION ext_oracle_raw TO `oi_ingenieros_datos`;
GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE ON EXTERNAL LOCATION ext_oracle_bronze  TO `oi_ingenieros_datos`;
GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE ON EXTERNAL LOCATION ext_oracle_silver  TO `oi_ingenieros_datos`;
GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE ON EXTERNAL LOCATION ext_oracle_gold    TO `oi_ingenieros_datos`;
GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE ON EXTERNAL LOCATION ext_oracle_control TO `oi_ingenieros_datos`;
-- Evidencia: Catalog Explorer → External data → cada ubicación → "Test connection" en verde (guardar captura)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Seguridad a nivel de fila y columna en gold
-- MAGIC Los consumidores de BI ven solo familias de estrategia con evidencia (F7) y no ven el USD neto por operación.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ${catalogo}.gold.filtro_familias(familia STRING)
  RETURN is_account_group_member('oi_ingenieros_datos') OR is_account_group_member('oi_analistas_riesgo') OR familia = 'F7';

ALTER TABLE ${catalogo}.gold.kpis_estrategias SET ROW FILTER ${catalogo}.gold.filtro_familias ON (familia);

CREATE OR REPLACE FUNCTION ${catalogo}.gold.mascara_usd(valor DOUBLE)
  RETURN CASE WHEN is_account_group_member('oi_consumidores_bi') THEN NULL ELSE valor END;

ALTER TABLE ${catalogo}.gold.kpis_estrategias ALTER COLUMN usd_neto SET MASK ${catalogo}.gold.mascara_usd;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA ${catalogo}.gold;
