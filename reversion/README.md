# reversion · Deshacer el despliegue

| Archivo | Qué elimina |
|---|---|
| `01_drop_medallion.py` | Reversión completa: filtro y máscara, funciones, tablas y esquemas en Unity Catalog (**lógico**) y las carpetas del catálogo en los contenedores `bronze`, `silver`, `gold` y `control` de ADLS (**físico**). Opcionalmente, el catálogo y las ubicaciones externas. |
| `02_drop_medallion.sql` | Solo la parte lógica, en SQL. |

Las tablas son externas: `DROP TABLE` borra la definición pero no los archivos, por eso hay un paso físico aparte.
El contenedor `raw` nunca se toca; con él y `proceso/1_preparacion_ambiente.py` el lakehouse se reconstruye.

El notebook en Python no hace nada si el parámetro `confirmar` no es `ELIMINAR` (dev) o `ELIMINAR <catálogo>`
(cualquier otro catálogo), para que producción no se revierta por dejar los valores por defecto.
