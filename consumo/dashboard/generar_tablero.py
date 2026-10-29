# -*- coding: utf-8 -*-
"""Genera la definición del tablero AI/BI (Lakeview) "Oracle Investment · Mercado y estrategias".

Uso:  python consumo/dashboard/generar_tablero.py [catalogo]  →  consumo/dashboard/tablero_oracle.lvdash.json
Luego se crea y publica con la CLI:  databricks lakeview create --display-name ... --serialized-dashboard @archivo
Las consultas son las de consultas_tablero.sql; el parámetro :anio se reemplaza por un filtro de año del tablero.
"""
import json
import sys
from pathlib import Path

CAT = sys.argv[1] if len(sys.argv) > 1 else "oracle_prd"

datasets = {
    "ds_mercado": f"SELECT anio, rendimiento_anual, volatilidad_anual, max_drawdown FROM {CAT}.gold.kpis_activo_anual WHERE ticker = 'SPY'",
    "ds_sector": f"SELECT anio, sector, rendimiento_promedio, volatilidad_promedio, peor_drawdown FROM {CAT}.gold.kpis_sector_anual",
    "ds_activos": (f"SELECT k.anio, k.ticker, c.sector, k.rendimiento_anual, k.volatilidad_anual, k.rendimiento_dividendo "
                   f"FROM {CAT}.gold.kpis_activo_anual k JOIN {CAT}.silver.catalogo_activos c USING (ticker) WHERE c.tipo <> 'Indice'"),
    "ds_estrategias_mes": f"SELECT mes, familia, usd_neto, operaciones FROM {CAT}.gold.kpis_estrategias",
    "ds_estrategias_regimen": (f"SELECT familia, regimen_vix, SUM(operaciones) AS operaciones, "
                               f"ROUND(SUM(ganancia_bruta) / NULLIF(SUM(perdida_bruta), 0), 2) AS profit_factor, "
                               f"ROUND(AVG(tasa_acierto), 3) AS tasa_acierto FROM {CAT}.gold.kpis_estrategias "
                               f"GROUP BY familia, regimen_vix ORDER BY familia, regimen_vix"),
    "ds_senales": (f"SELECT ticker, nombre, sector, fecha, ROUND(cierre, 2) AS cierre, ROUND(rsi2, 1) AS rsi2, senal_e1, senal_e2 "
                   f"FROM {CAT}.gold.panel_activos WHERE senal_e1 IS NOT NULL OR senal_e2 IS NOT NULL ORDER BY ticker"),
    "ds_calidad": (f"SELECT regla, ROUND(valor, 4) AS valor, umbral, cumple, fecha_utc FROM {CAT}.control.calidad "
                   f"QUALIFY ROW_NUMBER() OVER (PARTITION BY regla ORDER BY fecha_utc DESC) = 1"),
}


def campo(nombre, expr=None):
    return {"name": nombre, "expression": expr or f"`{nombre}`"}


def widget(nombre, dataset, campos, spec, pos, desagregado=True):
    return {"widget": {"name": nombre,
                       "queries": [{"name": "main_query", "query": {"datasetName": dataset, "fields": campos,
                                                                     "disaggregated": desagregado}}],
                       "spec": spec},
            "position": dict(zip(("x", "y", "width", "height"), pos))}


def contador(nombre, col, titulo, x):
    return widget(nombre, "ds_mercado", [campo(col)],
                  {"version": 2, "widgetType": "counter",
                   "encodings": {"value": {"fieldName": col, "displayName": titulo, "format": {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}}},
                   "frame": {"showTitle": True, "title": titulo, "showDescription": True, "description": "SPY en el año elegido"}},
                  (x, 1, 2, 3))


def tabla(nombre, dataset, columnas, titulo, pos):
    return widget(nombre, dataset, [campo(c) for c in columnas],
                  {"version": 2, "widgetType": "table",
                   "encodings": {"columns": [{"fieldName": c, "displayName": c} for c in columnas]},
                   "frame": {"showTitle": True, "title": titulo}}, pos)


filtro_anio = {
    "widget": {"name": "filtro_anio",
               "queries": [{"name": f"q_{d}", "query": {"datasetName": d, "fields": [campo("anio")], "disaggregated": False}}
                           for d in ("ds_mercado", "ds_sector", "ds_activos")],
               "spec": {"version": 2, "widgetType": "filter-single-select",
                        "encodings": {"fields": [{"fieldName": "anio", "displayName": "Año", "queryName": f"q_{d}"}
                                                 for d in ("ds_mercado", "ds_sector", "ds_activos")]},
                        "selection": {"defaultSelection": {"values": {"dataType": "INTEGER", "values": [{"value": "2025"}]}}},
                        "frame": {"showTitle": True, "title": "Año"}}},
    "position": {"x": 0, "y": 0, "width": 2, "height": 1}}

titulo = {"widget": {"name": "titulo", "multilineTextboxSpec": {"lines": [
    "## Oracle Investment · Mercado y estrategias\n",
    "Lakehouse medallion en Azure Databricks: KPIs de 33 activos (2015–2026) y de las familias de estrategia simuladas. "
    "Los datos de estrategias respetan el filtro de fila y la máscara de Unity Catalog del usuario que publica.\n"]}},
    "position": {"x": 2, "y": 0, "width": 4, "height": 1}}

widgets = [
    titulo, filtro_anio,
    contador("c_rend", "rendimiento_anual", "Rendimiento del mercado", 0),
    contador("c_vol", "volatilidad_anual", "Volatilidad anual", 2),
    contador("c_dd", "max_drawdown", "Caída máxima", 4),
    widget("b_sector", "ds_sector", [campo("sector"), campo("rendimiento_promedio")],
           {"version": 3, "widgetType": "bar",
            "encodings": {"x": {"fieldName": "rendimiento_promedio", "scale": {"type": "quantitative"}, "displayName": "Rendimiento promedio"},
                          "y": {"fieldName": "sector", "scale": {"type": "categorical", "sort": {"by": "x-reversed"}}, "displayName": "Sector"}},
            "frame": {"showTitle": True, "title": "Rendimiento promedio por sector"}}, (0, 4, 3, 8)),
    widget("s_riesgo", "ds_activos", [campo("ticker"), campo("sector"), campo("volatilidad_anual"), campo("rendimiento_anual")],
           {"version": 3, "widgetType": "scatter",
            "encodings": {"x": {"fieldName": "volatilidad_anual", "scale": {"type": "quantitative"}, "displayName": "Volatilidad anual"},
                          "y": {"fieldName": "rendimiento_anual", "scale": {"type": "quantitative"}, "displayName": "Rendimiento anual"},
                          "color": {"fieldName": "sector", "scale": {"type": "categorical"}, "displayName": "Sector"}},
            "frame": {"showTitle": True, "title": "Rendimiento vs. riesgo por activo"}}, (3, 4, 3, 8)),
    widget("l_familias", "ds_estrategias_mes", [campo("mes"), campo("familia"), campo("sum(usd_neto)", "SUM(`usd_neto`)")],
           {"version": 3, "widgetType": "line",
            "encodings": {"x": {"fieldName": "mes", "scale": {"type": "categorical"}, "displayName": "Mes"},
                          "y": {"fieldName": "sum(usd_neto)", "scale": {"type": "quantitative"}, "displayName": "USD neto"},
                          "color": {"fieldName": "familia", "scale": {"type": "categorical"}, "displayName": "Familia"}},
            "frame": {"showTitle": True, "title": "USD neto mensual por familia de estrategia (cuentas Sim)"}}, (0, 12, 6, 7),
           desagregado=False),
    tabla("t_pf", "ds_estrategias_regimen", ["familia", "regimen_vix", "operaciones", "profit_factor", "tasa_acierto"],
          "Profit factor por familia y régimen de volatilidad", (0, 19, 3, 8)),
    tabla("t_senales", "ds_senales", ["ticker", "sector", "fecha", "cierre", "rsi2", "senal_e1", "senal_e2"],
          "Señales vigentes de las estrategias base", (3, 19, 3, 8)),
    tabla("t_calidad", "ds_calidad", ["regla", "valor", "umbral", "cumple", "fecha_utc"],
          "Salud del pipeline: último control de calidad", (0, 27, 6, 6)),
]

tablero = {
    "datasets": [{"name": n, "displayName": n.replace("ds_", "").replace("_", " "), "queryLines": [q]} for n, q in datasets.items()],
    "pages": [{"name": "principal", "displayName": "Mercado y estrategias", "layout": widgets}],
}

salida = Path(__file__).with_name("tablero_oracle.lvdash.json")
salida.write_text(json.dumps(tablero, ensure_ascii=False, indent=1), encoding="utf-8")
print(f"{salida} · {len(datasets)} conjuntos de datos · {len(widgets)} elementos · catálogo {CAT}")
