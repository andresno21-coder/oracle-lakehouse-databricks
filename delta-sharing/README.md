# delta-sharing

Comparte la capa gold con un destinatario externo, sin copiar datos.

| Archivo | Qué hace |
|---|---|
| `01_compartir_gold.py` | Notebook PySpark: crea el share `oracle_kpis_mercado`, agrega 3 tablas de `gold` y da `SELECT` al destinatario abierto `socio_research`. |
| `02_leer_share.py` | Lectura desde fuera de Databricks con la librería `delta-sharing` y el archivo de credenciales `config.share`. |

El mismo guion en SQL está en [`seguridad/02_delta_sharing.sql`](../seguridad/02_delta_sharing.sql).
La evidencia de ejecución está en [`evidencias/README.md`](../evidencias/README.md), sección 10.

El archivo `config.share` y el enlace de activación son secretos: no se versionan.
