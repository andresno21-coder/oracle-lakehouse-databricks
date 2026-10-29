# -*- coding: utf-8 -*-
"""Genera el cuerpo para crear el espacio de Genie "Analista de mercado Oracle" (ver espacio_genie.md).

Uso:  python consumo/genie/generar_espacio.py <warehouse_id> [catalogo]  →  consumo/genie/espacio_genie.json
      databricks genie create-space --json @consumo/genie/espacio_genie.json
"""
import json
import sys
import uuid
from pathlib import Path

ALMACEN = sys.argv[1]
CAT = sys.argv[2] if len(sys.argv) > 2 else "oracle_prd"


def nuevo_id():
    return uuid.uuid4().hex


tablas = sorted([f"{CAT}.gold.kpis_activo_anual", f"{CAT}.gold.kpis_sector_anual", f"{CAT}.gold.kpis_estrategias",
                 f"{CAT}.gold.panel_activos", f"{CAT}.silver.catalogo_activos"])

instrucciones = """Responde en español. Los montos están en dólares estadounidenses y los porcentajes como fracción (0,12 = 12 %).
"Rendimiento" significa rendimiento_anual; "riesgo" significa volatilidad_anual; "caída máxima" es max_drawdown.
Las familias de estrategia se llaman F0 a F7; F0 agrupa familias menores. Una estrategia "tiene ventaja" si su profit factor (SUM(ganancia_bruta)/SUM(perdida_bruta)) es mayor que 1,3 con al menos 30 operaciones.
El régimen de volatilidad viene del VIX: BAJO (< 15), MEDIO (15–25), ALTO (> 25).
Señal E1 = seguimiento de tendencia (ruptura de 55 días); señal E2 = reversión a la media (RSI de 2 períodos < 10).
No hagas recomendaciones de inversión: describe los datos."""

preguntas = [
    "¿Qué sector tuvo el mejor rendimiento en 2025 y con qué volatilidad?",
    "¿Qué activos tienen hoy señal de compra de la estrategia E2?",
    "¿Qué familia de estrategia tiene profit factor mayor a 1,3 con al menos 30 operaciones?",
    "Compara el rendimiento por dividendo de KO, PEP y PG en los últimos tres años.",
    "¿Cómo le fue a la familia F1 según el régimen de volatilidad?",
]

sql_familias = (f"SELECT familia, SUM(operaciones) AS operaciones, SUM(ganancia_bruta)/NULLIF(SUM(perdida_bruta),0) AS profit_factor\n"
                f"FROM {CAT}.gold.kpis_estrategias GROUP BY familia\n"
                f"HAVING SUM(operaciones) >= 30 AND SUM(ganancia_bruta)/NULLIF(SUM(perdida_bruta),0) > 1.3")

espacio = {
    "version": 2,
    "config": {"sample_questions": sorted([{"id": nuevo_id(), "question": [p]} for p in preguntas], key=lambda q: q["id"])},
    "data_sources": {"tables": [{"identifier": t} for t in tablas]},
    "instructions": {
        "text_instructions": [{"id": nuevo_id(), "content": [linea + "\n" for linea in instrucciones.splitlines()]}],
        "example_question_sqls": [{"id": nuevo_id(), "question": ["¿Qué familias de estrategia tienen ventaja?"],
                                   "sql": [linea + "\n" for linea in sql_familias.splitlines()]}],
    },
}

cuerpo = {"warehouse_id": ALMACEN, "title": "Analista de mercado Oracle",
          "description": "Preguntas en lenguaje natural sobre los KPIs gold del lakehouse de Oracle Investment.",
          "serialized_space": json.dumps(espacio, ensure_ascii=False)}
salida = Path(__file__).with_name("espacio_genie.json")
salida.write_text(json.dumps(cuerpo, ensure_ascii=False, indent=1), encoding="utf-8")
print(f"{salida} · {len(tablas)} tablas · {len(preguntas)} preguntas de ejemplo")
