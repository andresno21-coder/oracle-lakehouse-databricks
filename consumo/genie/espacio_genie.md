# Espacio de Genie · "Analista de mercado Oracle"

**Tablas:** `oracle_prd.gold.kpis_activo_anual`, `oracle_prd.gold.kpis_sector_anual`, `oracle_prd.gold.kpis_estrategias`,
`oracle_prd.gold.panel_activos`, `oracle_prd.silver.catalogo_activos`.
**Almacén SQL:** serverless pequeño. **Acceso:** grupos `oi_consumidores_bi` y `oi_analistas_riesgo` (Genie respeta los
filtros de fila y las máscaras de Unity Catalog: un consumidor de BI no verá el USD neto).

## Instrucciones generales (pegar en *Instructions*)
- Responde en español. Los montos están en dólares estadounidenses y los porcentajes como fracción (0,12 = 12 %).
- "Rendimiento" significa `rendimiento_anual`; "riesgo" significa `volatilidad_anual`; "caída máxima" es `max_drawdown`.
- Las familias de estrategia se llaman F0 a F7; F0 agrupa familias menores. Una estrategia "tiene ventaja" si su
  `profit_factor` es mayor que 1,3 con al menos 30 operaciones.
- El régimen de volatilidad viene del VIX: BAJO (< 15), MEDIO (15–25), ALTO (> 25).
- Señal E1 = seguimiento de tendencia (ruptura de 55 días); señal E2 = reversión a la media (RSI de 2 períodos < 10).
- No hagas recomendaciones de inversión: describe los datos.

## Preguntas de ejemplo (para *Sample questions*)
1. ¿Qué sector tuvo el mejor rendimiento en 2025 y con qué volatilidad?
2. ¿Qué activos tienen hoy señal de compra de la estrategia E2?
3. ¿Qué familia de estrategia tiene profit factor mayor a 1,3 con al menos 30 operaciones?
4. Compara el rendimiento por dividendo de KO, PEP y PG en los últimos tres años.
5. ¿Cómo le fue a la familia F1 según el régimen de volatilidad?

## Consultas de referencia (*SQL instructions*)
```sql
-- Familias con ventaja
SELECT familia, SUM(operaciones) AS operaciones, SUM(ganancia_bruta)/NULLIF(SUM(perdida_bruta),0) AS profit_factor
FROM oracle_prd.gold.kpis_estrategias GROUP BY familia
HAVING SUM(operaciones) >= 30 AND SUM(ganancia_bruta)/NULLIF(SUM(perdida_bruta),0) > 1.3;
```
