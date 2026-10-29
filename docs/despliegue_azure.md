# Despliegue en Azure Databricks

## 1. Recursos de Azure (una vez)
```bash
az group create -n rg-oracle-lakehouse -l eastus2
az storage account create -n storacleinvestment -g rg-oracle-lakehouse --sku Standard_LRS --kind StorageV2 --hns true
az storage container create --account-name storacleinvestment -n raw --auth-mode login
az storage container create --account-name storacleinvestment -n lakehouse --auth-mode login
az databricks workspace create -n dbw-oracle-lakehouse -g rg-oracle-lakehouse -l eastus2 --sku premium
# Access Connector (identidad administrada) con permiso sobre la cuenta de almacenamiento
az databricks access-connector create -n ac-oracle-lakehouse -g rg-oracle-lakehouse -l eastus2 --identity-type SystemAssigned
az role assignment create --assignee <principalId-del-connector> --role "Storage Blob Data Contributor" \
  --scope /subscriptions/<sub>/resourceGroups/rg-oracle-lakehouse/providers/Microsoft.Storage/storageAccounts/storacleinvestment
```
Opcional para la exportación: Azure SQL Database (`sql-oracle`, base `oracle`) o PostgreSQL / Lakebase, y un Key Vault
con los secretos `sql-servidor`, `sql-base`, `db-usuario`, `db-clave` (o `pg-host`, `pg-base`).

## 2. Unity Catalog
1. Consola de cuenta → *Catalog* → el workspace queda asignado al metastore de la región.
2. En el workspace: *Catalog → External data → Credentials → Create* `cred_oracle_adls` con el Access Connector.
3. Ejecutar `notebooks/00_preparacion/00_preparacion_ambiente.sql` (crea ubicaciones externas, catálogo y esquemas).
4. Secret scope respaldado por Key Vault: `https://<workspace>#secrets/createScope` → nombre `oracle-kv`.
5. Crear grupos y usuarios ficticios (`seguridad/usuarios_ficticios.md`) y ejecutar `seguridad/01_grupos_y_permisos.sql`.

## 3. Cargar los insumos a ADLS
```bash
python insumos/descargar_insumos.py --salida insumos/datos
az storage blob upload-batch --account-name storacleinvestment -d raw -s insumos/datos --auth-mode login --overwrite
```

## 4. Desplegar con el bundle
```bash
databricks auth login --host https://adb-XXXX.azuredatabricks.net
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev oracle_etl
```

## 5. GitHub Actions
Secretos del repositorio: `DATABRICKS_HOST`, `DATABRICKS_TOKEN` (token de un service principal).
Crear el environment `prod` con revisores obligatorios para que el despliegue a producción requiera aprobación.

## 6. Consumo
- Tablero AI/BI: crear el tablero con las consultas de `consumo/dashboard/consultas_tablero.sql`.
- Genie: crear el espacio con `consumo/genie/espacio_genie.md`.
- App: `databricks apps create oracle-portal` y desplegar la carpeta `consumo/app` (asignar el recurso SQL warehouse).
- Delta Sharing: `seguridad/02_delta_sharing.sql`.

## Evidencias para la entrega
Guardar en `docs/evidencias/` las capturas de: ejecución exitosa del Job (grafo de tareas), tablas por capa en el
Catalog Explorer, linaje de una tabla gold, prueba de permisos con los usuarios ficticios, el tablero, Genie y la app.
