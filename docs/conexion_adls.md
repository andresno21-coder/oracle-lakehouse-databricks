# Conexión de Databricks con ADLS Gen2

| Método | Cómo funciona | Ventaja | Riesgo | Uso |
|---|---|---|---|---|
| Access key | La llave de la cuenta en la configuración de Spark | Simple | Da acceso total a toda la cuenta | Solo pruebas |
| SAS token | Token firmado con permisos y vencimiento | Permisos granulares | Vence y hay que rotarlo | Desarrollo |
| Service principal + Key Vault | App de Entra ID; secreto en un secret scope respaldado por Key Vault | Identidad propia, auditable | Gestionar y rotar el secreto | Alternativa productiva |
| **Managed Identity (elegido)** | Access Connector con rol *Storage Blob Data Contributor* → storage credential → external locations de Unity Catalog | Sin secretos; permisos gobernados con GRANT en Unity Catalog | — | **Producción** |

El montaje en DBFS (`dbutils.fs.mount`) está deprecado con Unity Catalog y no se usa.

## Configuración en este proyecto
1. Access Connector `ac-oracle-lakehouse` con rol *Storage Blob Data Contributor* sobre `storacleinvestment`.
2. Storage credential `cred_oracle_adls` (una sola credencial para todas las ubicaciones).
3. External locations, una por contenedor: `metastore`, `raw`, `bronze`, `silver`, `gold`, `control` (`00_preparacion_ambiente.sql`).
4. Permisos sobre las ubicaciones: `READ FILES` en raw y `READ/WRITE FILES` + `CREATE EXTERNAL TABLE` en las capas, solo para `oi_ingenieros_datos`.
5. Evidencia: *Test connection* en verde de cada ubicación (captura en `docs/evidencias/`).
