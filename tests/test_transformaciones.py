# -*- coding: utf-8 -*-
"""Pruebas unitarias de las transformaciones (Spark local, sin escribir a disco)."""
import datetime as dt
import pytest
from pyspark.sql import SparkSession
from oracle_lakehouse import transformaciones as tr


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests-oracle-lakehouse")
         .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def _precios(spark):
    filas = []
    for i, c in enumerate([100, 102, 101, 105, 103]):
        f = (dt.date(2025, 1, 6) + dt.timedelta(days=i)).isoformat()
        filas.append(("spy", f, float(c - 1), float(c + 1), float(c - 2), float(c), float(c), 1000.0))
    filas.append(("spy", "2025-01-06", 99.0, 101.0, 98.0, 100.0, 100.0, 1000.0))      # duplicado
    filas.append(("spy", "2025-01-13", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))                 # precio inválido
    for i, c in enumerate([14.0, 20.0, 30.0, 18.0, 12.0]):
        f = (dt.date(2025, 1, 6) + dt.timedelta(days=i)).isoformat()
        filas.append(("VIX", f, c, c, c, c, c, 0.0))
    return spark.createDataFrame(filas, "ticker string, fecha string, apertura double, maximo double, minimo double, cierre double, cierre_ajustado double, volumen double")


def test_normalizar_precios_quita_duplicados_e_invalidos(spark):
    p = tr.normalizar_precios(_precios(spark))
    spy = p.filter("ticker = 'SPY'")
    assert spy.count() == 5
    assert spy.filter("cierre_ajustado <= 0").count() == 0


def test_rendimientos(spark):
    p = tr.agregar_rendimientos(tr.normalizar_precios(_precios(spark))).filter("ticker = 'SPY'").orderBy("fecha").collect()
    assert p[0]["rendimiento_diario"] is None
    assert p[1]["rendimiento_diario"] == pytest.approx(0.02)


def test_regimen_vix(spark):
    v = {r["vix_cierre"]: r["regimen_vix"] for r in tr.regimen_vix(tr.normalizar_precios(_precios(spark))).collect()}
    assert v[14.0] == "BAJO" and v[20.0] == "MEDIO" and v[30.0] == "ALTO"


def test_operaciones_mapa_y_contexto(spark):
    raw = spark.createDataFrame([
        (1, "2025-01-07T15:00:00Z", "2025-01-07T16:00:00Z", "2025-01-07", "F1", "MES", "Long", 1, 1.0, 2.0, 1.0, 5.0, 4.0, 1.0, "Objetivo", 60.0),
        (2, "2025-01-12T23:00:00Z", "2025-01-12T23:30:00Z", "2025-01-12", "F1", "BTC", "Short", 1, 1.0, 2.0, -1.0, -5.0, 1.0, 4.0, "Stop", 30.0),
        (2, "2025-01-12T23:00:00Z", "2025-01-12T23:30:00Z", "2025-01-12", "F1", "BTC", "Short", 1, 1.0, 2.0, -1.0, -5.0, 1.0, 4.0, "Stop", 30.0),
    ], "id_operacion long, entrada_utc string, salida_utc string, fecha string, familia string, simbolo string, lado string, qty long, precio_entrada double, precio_salida double, puntos double, usd_neto double, mfe_ticks double, mae_ticks double, resultado string, duracion_min double")
    mapa = spark.createDataFrame([("MES", "Micro S&P", "SPY", 5.0)], "simbolo_futuro string, nombre_futuro string, ticker_subyacente string, valor_punto_usd double")
    ops = tr.normalizar_operaciones(raw, mapa)
    assert ops.count() == 2                                   # duplicado eliminado
    assert ops.filter("con_subyacente").count() == 1          # BTC queda sin subyacente
    precios = tr.agregar_rendimientos(tr.normalizar_precios(_precios(spark)))
    ctx = {r["id_operacion"]: r for r in tr.contexto_de_mercado(ops, precios).collect()}
    assert ctx[1]["regimen_vix"] == "MEDIO" and ctx[1]["rendimiento_subyacente"] == pytest.approx(0.02)
    assert ctx[2]["fecha_mercado"] == dt.date(2025, 1, 10)    # domingo -> viernes anterior
    assert ctx[2]["regimen_vix"] == "BAJO"


def test_kpis_estrategias_profit_factor(spark):
    ops = spark.createDataFrame([("F1", dt.date(2025, 1, 6), "BAJO", 10.0, True), ("F1", dt.date(2025, 1, 7), "BAJO", -5.0, False)],
                                "familia string, fecha date, regimen_vix string, usd_neto double, gana boolean")
    k = tr.kpis_estrategias(ops).collect()[0]
    assert k["operaciones"] == 2 and k["profit_factor"] == pytest.approx(2.0) and k["tasa_acierto"] == pytest.approx(0.5)


def test_kpis_activo_anual_drawdown(spark):
    p = tr.agregar_rendimientos(tr.normalizar_precios(_precios(spark))).filter("ticker = 'SPY'")
    div = spark.createDataFrame([("SPY", dt.date(2025, 1, 8), 1.05)], "ticker string, fecha_pago date, dividendo double")
    k = tr.kpis_activo_anual(p, div).collect()[0]
    assert k["rendimiento_anual"] == pytest.approx(0.03)
    assert k["max_drawdown"] == pytest.approx(103 / 105 - 1)
    assert k["rendimiento_dividendo"] == pytest.approx(1.05 / 103)
