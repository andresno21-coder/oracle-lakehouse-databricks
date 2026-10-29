# Usuarios ficticios del proyecto

Se crean en la consola de cuenta (`accounts.azuredatabricks.net` → *User management* → *Users*), se agregan a su grupo
y el grupo se asigna al workspace. No se usan correos reales: el dominio `oracleinvestment.example` es reservado.

| Usuario | Grupo | Qué debe poder hacer | Qué no debe poder hacer |
|---|---|---|---|
| `ana.ingeniera@oracleinvestment.example` | `oi_ingenieros_datos` | Ejecutar el Job, crear y modificar tablas en todas las capas | — |
| `luis.riesgo@oracleinvestment.example` | `oi_analistas_riesgo` | Consultar silver y gold, ver todas las familias y el USD neto | Leer bronze o los archivos raw |
| `marta.bi@oracleinvestment.example` | `oi_consumidores_bi` | Consultar gold desde el tablero, Genie y la app | Ver silver; ver familias sin evidencia; ver USD neto |
| `jorge.auditor@oracleinvestment.example` | `oi_auditores` | Consultar gold y las bitácoras de control | Modificar datos |

## Prueba de permisos (evidencia para la entrega)
1. Iniciar sesión como `marta.bi` y ejecutar `SELECT * FROM oracle_dev.silver.precios_diarios` → **PERMISSION_DENIED**.
2. Como `marta.bi`: `SELECT familia, usd_neto FROM oracle_dev.gold.kpis_estrategias` → solo filas `F7` y `usd_neto` en NULL.
3. Como `luis.riesgo`: la misma consulta devuelve todas las familias con su USD neto.
4. Guardar capturas de los tres resultados en `docs/evidencias/`.
