# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Servir · Exportar gold a la base transaccional
# MAGIC Copia las tablas gold a una base relacional para las aplicaciones y el reporte (Power BI):
# MAGIC **Azure SQL Database** (por defecto) o **PostgreSQL / Lakebase**. Las credenciales viven en un *secret scope*
# MAGIC respaldado por Azure Key Vault (`oracle-kv`); nunca en el código.

# COMMAND ----------

# MAGIC %run ../_comun

# COMMAND ----------

dbutils.widgets.dropdown("motor", "azuresql", ["azuresql", "postgres"], "Base destino")
MOTOR = dbutils.widgets.get("motor")
ESCOPO = "oracle-kv"

if MOTOR == "azuresql":
    url = (f"jdbc:sqlserver://{dbutils.secrets.get(ESCOPO, 'sql-servidor')}.database.windows.net:1433;"
           f"database={dbutils.secrets.get(ESCOPO, 'sql-base')};encrypt=true;loginTimeout=30;")
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
else:
    url = f"jdbc:postgresql://{dbutils.secrets.get(ESCOPO, 'pg-host')}:5432/{dbutils.secrets.get(ESCOPO, 'pg-base')}?sslmode=require"
    driver = "org.postgresql.Driver"
props = {"user": dbutils.secrets.get(ESCOPO, "db-usuario"), "password": dbutils.secrets.get(ESCOPO, "db-clave"), "driver": driver}

# COMMAND ----------

# El esquema destino se crea una vez; Spark JDBC crea las tablas pero no los esquemas
ddl = ("IF SCHEMA_ID('oracle') IS NULL EXEC('CREATE SCHEMA oracle')" if MOTOR == "azuresql"
       else "CREATE SCHEMA IF NOT EXISTS oracle")
jvm = spark.sparkContext._gateway.jvm
jvm.java.lang.Class.forName(driver)
conexion = jvm.java.sql.DriverManager.getConnection(url, props["user"], props["password"])
try:
    conexion.createStatement().execute(ddl)
finally:
    conexion.close()

# COMMAND ----------

for nombre in ["kpis_activo_anual", "kpis_sector_anual", "kpis_estrategias", "panel_activos"]:
    df = spark.table(tabla("gold", nombre))
    (df.write.mode("overwrite").option("truncate", "true").option("batchsize", 5000)
       .jdbc(url=url, table=f"oracle.{nombre}", properties=props))
    registrar_ejecucion(f"exportar_{nombre}", f"{MOTOR}:oracle.{nombre}", df.count())
print("Exportación completa")
