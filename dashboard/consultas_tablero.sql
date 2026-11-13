-- Consultas del tablero AI/BI "Oracle Investment · Mercado y estrategias" (también sirven para Power BI)
-- Parámetro del tablero: :anio (por defecto, el año en curso)

-- 1. Tarjetas: rendimiento y riesgo del mercado (SPY) en el año
SELECT rendimiento_anual, volatilidad_anual, max_drawdown
FROM oracle_prd.gold.kpis_activo_anual WHERE ticker = 'SPY' AND anio = :anio;

-- 2. Barras: rendimiento promedio por sector en el año
SELECT sector, rendimiento_promedio, volatilidad_promedio, peor_drawdown
FROM oracle_prd.gold.kpis_sector_anual WHERE anio = :anio ORDER BY rendimiento_promedio DESC;

-- 3. Dispersión rendimiento vs. riesgo por activo
SELECT k.ticker, c.sector, k.rendimiento_anual, k.volatilidad_anual, k.rendimiento_dividendo
FROM oracle_prd.gold.kpis_activo_anual k JOIN oracle_prd.silver.catalogo_activos c USING (ticker)
WHERE k.anio = :anio AND c.tipo <> 'Indice';

-- 4. Línea: USD neto mensual por familia de estrategia
SELECT mes, familia, SUM(usd_neto) AS usd_neto, SUM(operaciones) AS operaciones
FROM oracle_prd.gold.kpis_estrategias GROUP BY mes, familia ORDER BY mes;

-- 5. Tabla: profit factor por familia y régimen de volatilidad
SELECT familia, regimen_vix, SUM(operaciones) AS operaciones,
       SUM(ganancia_bruta) / NULLIF(SUM(perdida_bruta), 0) AS profit_factor, AVG(tasa_acierto) AS tasa_acierto
FROM oracle_prd.gold.kpis_estrategias GROUP BY familia, regimen_vix ORDER BY familia, regimen_vix;

-- 6. Tabla: señales vigentes de las estrategias base (último día)
SELECT ticker, nombre, sector, fecha, cierre, rsi2, senal_e1, senal_e2
FROM oracle_prd.gold.panel_activos WHERE senal_e1 IS NOT NULL OR senal_e2 IS NOT NULL ORDER BY ticker;

-- 7. Salud del pipeline: última ejecución y controles de calidad
SELECT paso, MAX(fecha_utc) AS ultima_ejecucion, MAX_BY(filas, fecha_utc) AS filas
FROM oracle_prd.control.ejecuciones GROUP BY paso ORDER BY paso;
SELECT regla, valor, umbral, cumple FROM oracle_prd.control.calidad
QUALIFY ROW_NUMBER() OVER (PARTITION BY regla ORDER BY fecha_utc DESC) = 1;
