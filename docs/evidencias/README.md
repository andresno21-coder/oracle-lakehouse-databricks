# Evidencias de la ejecución en Azure Databricks

Despliegue real del 29-oct-2026 en la suscripción de prueba del autor (región East US 2).

| Recurso | Nombre |
|---|---|
| Grupo de recursos | `rg-oracle-lakehouse` |
| ADLS Gen2 | `storacleinvestment` (contenedores `metastore`, `raw`, `bronze`, `silver`, `gold`, `control`) |
| Access Connector (Managed Identity) | `ac-oracle-lakehouse` → rol *Storage Blob Data Contributor* |
| Workspace Premium | `dbw-oracle-lakehouse` (Unity Catalog, metastore de la región) |
| Storage credential | `cred_oracle_adls` (validación: READ, LIST, WRITE, DELETE, PATH_EXISTS, HNS → PASS) |
| Base transaccional | Azure SQL Database `oracle` (oferta gratuita, Central US) |
| Secret scope | `oracle-kv` (servidor, base, usuario y clave de la base) |

## 1. Job orquestado (12 tareas)
`01_job_prod_12_tareas_ok.jpg`: corrida de producción con las 12 tareas en verde (15 min, clúster de un nodo).

| Corrida | Target | Resultado |
|---|---|---|
| 441744510934445 | dev | 11/12 OK; falló `mantenimiento_delta` (SHALLOW CLONE no admite tablas externas en UC) → corregido a DEEP CLONE |
| 215222858756058 | dev | **12/12 OK** |
| 81387748511210 | prod | **12/12 OK** |

## 2. Tablas por capa (todas EXTERNAS en ADLS)
| Tabla | Filas |
|---|---|
| bronze.precios | 104.056 |
| bronze.operaciones | 17.169 |
| silver.precios_diarios | 104.056 |
| silver.operaciones | 17.169 |
| gold.kpis_activo_anual | 420 |
| gold.kpis_sector_anual | 264 |
| gold.kpis_estrategias | 50 |
| gold.senales_estrategias_base | 104.056 |

Las 18 tablas del catálogo aparecen como `EXTERNAL` en `information_schema.tables`.

## 3. Ingesta incremental idempotente
Bitácora `control.ejecuciones`: la primera corrida cargó 104.056 precios, 17.169 operaciones, 1.486 dividendos,
35 activos y 18 futuros; la segunda corrida, sin archivos nuevos en `raw`, cargó **0 filas** en las cinco tablas bronze.

## 4. Calidad de datos
Las 7 reglas de `control.calidad` se cumplen en todas las corridas (precios duplicados 0, precios no positivos 0,
OHLC inconsistente 0 %, tickers sin catálogo 0, operaciones sin subyacente 5,4 % < 10 %, sin régimen 1,7 % < 5 %,
días desde el último precio 1 ≤ 5).

## 5. Exportación a la base transaccional
`exportar_base_transaccional` escribió en Azure SQL `oracle.kpis_activo_anual` (420), `oracle.kpis_sector_anual` (264),
`oracle.kpis_estrategias` (50) y `oracle.panel_activos` (35), con credenciales leídas del secret scope.

## 6. Seguridad: grupos, usuarios ficticios, filtro de fila y máscara
Grupos de cuenta `oi_ingenieros_datos`, `oi_analistas_riesgo`, `oi_consumidores_bi`, `oi_auditores` con un usuario
ficticio cada uno (`*@oracleinvestment.example`). 19 GRANT aplicados por catálogo (dev y prod).
Prueba sobre `gold.kpis_estrategias` con el mismo usuario, cambiando solo su pertenencia a grupos:

| Pertenencia del usuario | Filas | Familias visibles | `usd_neto` visible |
|---|---|---|---|
| ningún grupo de negocio | 6 | F7 | 6 de 6 |
| `oi_consumidores_bi` | 6 | F7 | **0 de 6 (enmascarado)** |
| `oi_analistas_riesgo` | 50 | F0–F7 | 50 de 50 |

## 7. Tablero AI/BI
`05_tablero_kpis_mercado.jpg` y `06_tablero_tablas_y_calidad.jpg`: tablero publicado con filtro de año, tarjetas del
mercado, rendimiento por sector, rendimiento vs. riesgo, USD neto por familia, profit factor por régimen, señales
vigentes y salud del pipeline. Definición reproducible con `consumo/dashboard/generar_tablero.py`.

## 8. Genie
Espacio "Analista de mercado Oracle" (`consumo/genie/generar_espacio.py`). Pregunta: *¿Qué sector tuvo el mejor
rendimiento en 2025 y con qué volatilidad?* → Genie generó SQL sobre `oracle_prd.gold.kpis_sector_anual` y respondió
**ETF Commodities Focused, rendimiento 63,5 %, volatilidad 27,6 %**, que coincide con el tablero.

## 9. Databricks App
`oracle-portal` desplegada (estado *App is running*) con el recurso SQL warehouse y un service principal
propio con permisos mínimos: `SELECT` en gold y `MODIFY` solo en `control.solicitudes_acceso`.
`07_app_portal_panel.jpg`: la app, tras el consentimiento OAuth del usuario, muestra el rendimiento promedio por sector
y las señales vigentes de las estrategias base leídos de `oracle_prd.gold`.

## 10. Delta Sharing
`seguridad/02_delta_sharing.sql` sobre `oracle_prd`: share `oracle_kpis_mercado` con `gold.kpis_activo_anual` y
`gold.kpis_sector_anual` (con historial) y `gold.panel_activos`; destinatario abierto `socio_research` con `SELECT`.
El metastore quedó con alcance `INTERNAL_AND_EXTERNAL`.

Lectura desde fuera de Databricks (`seguridad/leer_delta_sharing.py`, Python 3.11 local con `delta-sharing`, credencial
descargada del enlace de activación y no versionada):

| Tabla compartida | Filas leídas |
|---|---|
| `oracle_kpis_mercado.gold.panel_activos` | 35 |
| `oracle_kpis_mercado.gold.kpis_activo_anual` | 420 |
| `oracle_kpis_mercado.gold.kpis_sector_anual` | 264 |

Top 2025 leído por el socio: ETF Commodities Focused (63,5 %, volatilidad 27,6 %), Healthcare (48,1 %),
Financial Services (37,1 %): las mismas cifras que el tablero y Genie.
