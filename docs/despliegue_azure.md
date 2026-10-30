# Despliegue en Azure Databricks

Pasos usados en el despliegue real (29-oct-2026, suscripción de prueba). Resultados en `docs/evidencias/README.md`.

## 1. Recursos de Azure (una vez)
```bash
az login --use-device-code --tenant <tenant-id>
az group create -n rg-oracle-lakehouse -l eastus2
az storage account create -n storacleinvestment -g rg-oracle-lakehouse -l eastus2 --sku Standard_LRS --kind StorageV2 --hns true
for c in metastore raw bronze silver gold control; do
  az storage container create --account-name storacleinvestment -n $c --auth-mode login
done
az databricks workspace create -n dbw-oracle-lakehouse -g rg-oracle-lakehouse -l eastus2 --sku premium
# Access Connector (identidad administrada) con permiso sobre la cuenta de almacenamiento
az databricks access-connector create -n ac-oracle-lakehouse -g rg-oracle-lakehouse -l eastus2 --identity-type SystemAssigned
az role assignment create --assignee-object-id <principalId-del-connector> --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" --scope <id-de-la-cuenta-de-almacenamiento>
# Base transaccional: oferta gratuita de Azure SQL (en la prueba, East US 2 no acepta servidores SQL nuevos → centralus)
az sql server create -n sql-oracle-XXXX -g rg-oracle-lakehouse -l centralus --admin-user oracle_etl --admin-password <clave>
az sql server firewall-rule create -g rg-oracle-lakehouse -s sql-oracle-XXXX -n PermitirServiciosAzure --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0
az sql db create -g rg-oracle-lakehouse -s sql-oracle-XXXX -n oracle -e GeneralPurpose -f Gen5 -c 2 --compute-model Serverless \
  --use-free-limit --free-limit-exhaustion-behavior AutoPause
```
**Cuota de la suscripción de prueba:** 4 vCPU por región y, en East US 2, solo la generación v7 de máquinas habilitada.
Por eso el Job usa un clúster de un solo nodo `Standard_D4ds_v7` (variable `nodo` del bundle).

## 2. Unity Catalog
1. El workspace nuevo queda asignado automáticamente al metastore de la región.
2. Storage credential con el Access Connector:
   `databricks storage-credentials create --json '{"name":"cred_oracle_adls","azure_managed_identity":{"access_connector_id":"<id>"}}'`
3. Las ubicaciones externas, el catálogo, los esquemas y las 17 tablas externas los crea la primera tarea del Job
   (`notebooks/00_preparacion/00_preparacion_ambiente.sql`).
4. Secret scope `oracle-kv` con `sql-servidor`, `sql-base`, `db-usuario`, `db-clave`
   (`databricks secrets create-scope oracle-kv` y `databricks secrets put-secret ...`).
5. Grupos y usuarios ficticios **de cuenta** (Unity Catalog no reconoce grupos locales del workspace): API SCIM de
   cuenta expuesta por el workspace, `/api/2.0/account/scim/v2/Groups`. Luego `seguridad/01_grupos_y_permisos.sql`.
6. Delta Sharing requiere `CREATE SHARE` y `CREATE RECIPIENT` sobre el metastore: en la consola de cuenta
   (accounts.azuredatabricks.net → Catalog → metastore) se cambia el **Metastore admin** al usuario del workspace y se
   habilita compartir fuera de la organización (alcance `INTERNAL_AND_EXTERNAL`).
   Con una cuenta personal de Microsoft (`@live.com`, `@outlook.com`) la consola de cuenta rechaza el inicio de sesión
   ("la cuenta no existe en el inquilino Microsoft Services"): se crea en Entra ID un usuario nativo
   (`admin@<inquilino>.onmicrosoft.com`) con rol **Administrador global** y se entra con él solo para este paso.

## 3. Cargar los insumos a ADLS
```bash
python insumos/descargar_insumos.py --salida insumos/datos
az storage blob upload-batch --account-name storacleinvestment -d raw -s insumos/datos --auth-mode login --overwrite
```

## 4. Desplegar con el bundle
`~/.databrickscfg` con un perfil `auth_type = azure-cli` (reutiliza el `az login`):
```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev oracle_etl
databricks bundle deploy -t prod && databricks bundle run -t prod oracle_etl
```
dev y prod comparten workspace y se aíslan por catálogo (`oracle_dev` / `oracle_prd`) y por ruta del bundle.

## 5. GitHub Actions
Secretos del repositorio: `DATABRICKS_HOST`, `DATABRICKS_TOKEN` (token de un service principal).
Crear el environment `prod` con revisores obligatorios para que el despliegue a producción requiera aprobación.

## 6. Consumo
- Tablero AI/BI: `python consumo/dashboard/generar_tablero.py oracle_prd` → `databricks lakeview create` y `publish`.
- Genie: `python consumo/genie/generar_espacio.py <warehouse_id> oracle_prd` → `databricks genie create-space --json @consumo/genie/espacio_genie.json`.
- App: `databricks apps create` con el recurso SQL warehouse, `databricks sync consumo/app <ruta>` y
  `databricks apps deploy oracle-portal --source-code-path <ruta>`. Al service principal de la app: `USE CATALOG`,
  `SELECT` en gold y `MODIFY` en `control.solicitudes_acceso`.
- Delta Sharing: `seguridad/02_delta_sharing.sql`.

## Costos
La suscripción de prueba tiene límite de gasto: no se cobra a la tarjeta. El SQL warehouse serverless se apaga solo a
los 10 minutos y el clúster del Job termina con cada corrida. La app consume mientras está encendida:
`databricks apps stop oracle-portal` cuando no se use.
