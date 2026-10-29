# Diccionario de datos

## bronze (copia fiel + auditoría)
Todas las tablas agregan `_fecha_ingesta`, `_origen`, `_archivo_origen` y `_archivo_modificado`.
`precios`, `dividendos`, `catalogo`, `mapa_futuros`, `operaciones`: mismas columnas que el insumo (texto en CSV).

## silver
| Tabla | Clave | Columnas principales |
|---|---|---|
| `catalogo_activos` | ticker | nombre, tipo (Accion/ETF/Indice), sector, industria, capitalizacion_usd |
| `precios_diarios` | ticker, fecha | apertura, maximo, minimo, cierre, cierre_ajustado, volumen, rendimiento_diario, rendimiento_log, anio, calidad_ohlc_ok, nombre, tipo, sector, industria |
| `dividendos` | ticker, fecha_pago | dividendo |
| `operaciones` | id_operacion | entrada_utc, salida_utc, fecha, fecha_mercado, familia (F0–F7), simbolo, lado (LARGO/CORTO), contratos, precios, puntos, usd_neto, mfe_ticks, mae_ticks, resultado, duracion_min, ticker_subyacente, valor_punto_usd, con_subyacente, gana, rendimiento_subyacente, vix_cierre, regimen_vix |

## gold
| Tabla | Grano | Métricas |
|---|---|---|
| `kpis_activo_anual` | ticker × año | sesiones, rendimiento_anual, volatilidad_anual (desv. diaria × √252), max_drawdown, dividendos_anio, rendimiento_dividendo, ratio_rend_riesgo |
| `kpis_sector_anual` | sector × año | activos, rendimiento_promedio, volatilidad_promedio, peor_drawdown, rendimiento_dividendo_promedio |
| `kpis_estrategias` | familia × mes × régimen VIX | operaciones, tasa_acierto, ganancia_bruta, perdida_bruta, usd_neto, expectativa_usd, profit_factor |
| `senales_estrategias_base` | ticker × día | sma200, max55, min55, rsi2, senal_e1 (LARGO/CORTO), senal_e2 (COMPRA) |
| `panel_activos` | ticker | última señal + KPIs del año en curso + sector |

## control
`ejecuciones` (paso, tabla, filas, estado por corrida) · `calidad` (regla, valor, umbral, crítica, cumple) ·
`solicitudes_acceso` (autoservicio de la app).

## Definiciones
- **Profit factor:** ganancia bruta / pérdida bruta. **Expectativa:** USD neto promedio por operación.
- **Régimen VIX:** BAJO < 15 ≤ MEDIO ≤ 25 < ALTO. Operaciones del fin de semana se asignan a la sesión del viernes.
- **E1:** cierre > máximo de 55 sesiones previas y > media de 200 (largo); simétrico para corto.
- **E2:** RSI de 2 sesiones < 10 con cierre > media de 200 (compra de sobreventa).
