# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Controles de calidad
# MAGIC Reglas que se evalúan al final de cada corrida; el resultado queda en `control.calidad`.
# MAGIC Si una regla **crítica** falla, el notebook termina con error y el Job no publica (no se ejecuta la exportación).

# COMMAND ----------

# MAGIC %run ../_comun

# COMMAND ----------

import datetime as dt

p = spark.table(tabla("silver", "precios_diarios"))
o = spark.table(tabla("silver", "operaciones"))
reglas = [
    # (nombre, valor medido, umbral, crítica)
    ("precios_duplicados", p.groupBy("ticker", "fecha").count().filter("count > 1").count(), 0, True),
    ("precios_no_positivos", p.filter("cierre_ajustado <= 0").count(), 0, True),
    ("precios_ohlc_inconsistentes_pct", p.filter("not calidad_ohlc_ok").count() / max(p.count(), 1), 0.01, False),
    ("tickers_sin_catalogo", p.filter("sector is null").select("ticker").distinct().count(), 0, False),
    ("operaciones_sin_subyacente_pct", o.filter("not con_subyacente").count() / max(o.count(), 1), 0.10, False),
    ("operaciones_sin_regimen_pct", o.filter("regimen_vix is null").count() / max(o.count(), 1), 0.05, False),
    ("dias_desde_ultimo_precio", (dt.date.today() - p.agg(F.max("fecha")).first()[0]).days, 5, True),
]
filas = [(dt.datetime.utcnow(), n, float(v), float(u), c, float(v) <= float(u)) for n, v, u, c in reglas]
res = spark.createDataFrame(filas, "fecha_utc timestamp, regla string, valor double, umbral double, critica boolean, cumple boolean")
res.write.mode("append").saveAsTable(tabla("control", "calidad"))
display(res)

fallas = [r.regla for r in res.filter("critica and not cumple").collect()]
if fallas:
    raise Exception(f"Controles críticos fallidos: {fallas}")
