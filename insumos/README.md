# Insumos (capa raw)

`descargar_insumos.py` genera las cinco fuentes en `insumos/datos/` con la misma estructura de carpetas que tendrá el
contenedor `raw` de ADLS:

```
raw/
├── precios/precios_diarios_<AAAA>.csv
├── dividendos/dividendos.json
├── catalogo/catalogo_activos.csv
├── catalogo/mapa_futuros.csv
└── operaciones/operaciones.parquet
```

Subida a ADLS:
```bash
az storage blob upload-batch --account-name storacleinvestment -d raw -s insumos/datos --auth-mode login --overwrite
```

Notas:
- Precios y dividendos: Yahoo Finance (uso educativo). Si hay un proxy TLS corporativo, definir `CURL_CA_BUNDLE`.
- Operaciones: exportadas de la bitácora de simulación de Oracle Investment; las familias de estrategia se publican con
  códigos genéricos (F0–F7) y los identificadores se renumeran.
- Los archivos de `insumos/datos/` se incluyen en el repositorio para reproducir el proyecto sin depender de la API.
