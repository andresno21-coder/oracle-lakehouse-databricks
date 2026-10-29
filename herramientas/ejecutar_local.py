# -*- coding: utf-8 -*-
"""Ejecuta el pipeline completo en Spark local sobre insumos/datos (validación antes de Databricks).

No escribe Delta: calcula cada capa en memoria, imprime conteos y guarda muestras de gold en
docs/muestras/ (CSV) para documentar los KPIs. Uso: python herramientas/ejecutar_local.py
"""
import json, os, sys
from pathlib import Path
RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
from pyspark.sql import SparkSession, functions as F
from oracle_lakehouse import transformaciones as tr

D = RAIZ / "insumos" / "datos"
spark = (SparkSession.builder.master("local[4]").appName("oracle-lakehouse-local")
         .config("spark.sql.shuffle.partitions", "8").config("spark.ui.enabled", "false")
         .config("spark.driver.memory", "4g").getOrCreate())

# bronze (lectura con esquema explícito, igual que en los notebooks)
import glob
import pandas as pd

def leer(ruta, fmt):
    """En local se lee con pandas (Spark en Windows no lista carpetas sin winutils); en Databricks se lee de ADLS."""
    archivos = sorted(glob.glob(str(ruta))) if any(ch in str(ruta) for ch in "*?") else [str(ruta)]
    if fmt == "csv": pdf = pd.concat(pd.read_csv(a, dtype=str) for a in archivos)
    elif fmt == "json": pdf = pd.concat(pd.read_json(a, lines=True, dtype=False) for a in archivos)
    else: pdf = pd.concat(pd.read_parquet(a) for a in archivos)
    pdf = pdf.astype(object).where(pd.notnull(pdf), None)
    return spark.createDataFrame(pdf.astype(str).where(pdf.notnull(), None) if fmt == "csv" else pdf)

b_precios = tr.agregar_auditoria(leer(D / "precios" / "*.csv", "csv"), "precios")
b_div = tr.agregar_auditoria(leer(D / "dividendos" / "dividendos.json", "json"), "dividendos")
b_cat = tr.agregar_auditoria(leer(D / "catalogo" / "catalogo_activos.csv", "csv"), "catalogo")
b_mapa = tr.agregar_auditoria(leer(D / "catalogo" / "mapa_futuros.csv", "csv"), "mapa_futuros")
b_ops = tr.agregar_auditoria(leer(D / "operaciones" / "operaciones.parquet", "parquet"), "operaciones")

# silver
s_cat = tr.normalizar_catalogo(b_cat)
s_precios = tr.enriquecer_con_catalogo(tr.agregar_rendimientos(tr.normalizar_precios(b_precios)), s_cat).cache()
s_div = tr.normalizar_dividendos(b_div)
s_ops = tr.contexto_de_mercado(tr.normalizar_operaciones(b_ops, b_mapa), s_precios).cache()

# gold
g_activo = tr.kpis_activo_anual(s_precios, s_div).cache()
g_sector = tr.kpis_sector_anual(g_activo, s_cat)
g_estr = tr.kpis_estrategias(s_ops)
g_senal = tr.senales_estrategias_base(s_precios)

conteos = {
    "bronze.precios": b_precios.count(), "bronze.dividendos": b_div.count(), "bronze.catalogo": b_cat.count(),
    "bronze.mapa_futuros": b_mapa.count(), "bronze.operaciones": b_ops.count(),
    "silver.precios": s_precios.count(), "silver.dividendos": s_div.count(), "silver.operaciones": s_ops.count(),
    "silver.operaciones_sin_subyacente": s_ops.filter(~F.col("con_subyacente")).count(),
    "silver.precios_ohlc_inconsistente": s_precios.filter(~F.col("calidad_ohlc_ok")).count(),
    "gold.kpis_activo_anual": g_activo.count(), "gold.kpis_sector_anual": g_sector.count(),
    "gold.kpis_estrategias": g_estr.count(), "gold.senales_estrategias_base": g_senal.filter("senal_e1 is not null or senal_e2 is not null").count(),
}
print(json.dumps(conteos, indent=1, ensure_ascii=False))

out = RAIZ / "docs" / "muestras"; out.mkdir(parents=True, exist_ok=True)
g_activo.filter("anio = 2025").orderBy(F.desc("ratio_rend_riesgo")).toPandas().round(4).to_csv(out / "gold_kpis_activo_2025.csv", index=False)
g_sector.filter("anio = 2025").orderBy(F.desc("rendimiento_promedio")).toPandas().round(4).to_csv(out / "gold_kpis_sector_2025.csv", index=False)
(g_estr.groupBy("familia", "regimen_vix").agg(F.sum("operaciones").alias("operaciones"), F.sum("usd_neto").alias("usd_neto"),
                                              (F.sum("ganancia_bruta") / F.sum("perdida_bruta")).alias("profit_factor"))
       .orderBy("familia", "regimen_vix").toPandas().round(3).to_csv(out / "gold_kpis_estrategias_por_regimen.csv", index=False))
ult = g_senal.agg(F.max("fecha")).first()[0]
g_senal.filter(F.col("fecha") == ult).orderBy("ticker").toPandas().round(3).to_csv(out / "gold_senales_ultimo_dia.csv", index=False)
(out / "conteos_capas.json").write_text(json.dumps(conteos, indent=1, ensure_ascii=False), encoding="utf-8")
print("muestras en", out)
spark.stop()
