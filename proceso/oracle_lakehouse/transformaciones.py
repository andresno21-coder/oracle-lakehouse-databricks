# -*- coding: utf-8 -*-
"""Transformaciones del lakehouse de Oracle Investment (PySpark puro, sin Spark SQL).

Cada función recibe y devuelve DataFrames, sin leer ni escribir: así los notebooks de Databricks
las orquestan y las pruebas de tests/ las validan en local con datos pequeños.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------- utilidades

def agregar_auditoria(df: DataFrame, origen: str) -> DataFrame:
    """Columnas técnicas de la capa bronze: cuándo y de dónde llegó cada fila."""
    return (df.withColumn("_fecha_ingesta", F.current_timestamp())
              .withColumn("_origen", F.lit(origen)))


def _dedup(df: DataFrame, claves, orden_col: str) -> DataFrame:
    w = Window.partitionBy(*claves).orderBy(F.col(orden_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# ---------------------------------------------------------------- silver: precios

def normalizar_precios(raw: DataFrame) -> DataFrame:
    """Tipos correctos, una fila por (ticker, fecha), sin precios no positivos y con banderas de calidad."""
    df = (raw.select(
            F.upper(F.trim("ticker")).alias("ticker"),
            F.to_date("fecha").alias("fecha"),
            *[F.col(c).cast("double").alias(c) for c in ("apertura", "maximo", "minimo", "cierre", "cierre_ajustado")],
            F.col("volumen").cast("double").cast("long").alias("volumen"),
            *([F.col("_fecha_ingesta")] if "_fecha_ingesta" in raw.columns else [F.current_timestamp().alias("_fecha_ingesta")]))
          .filter(F.col("ticker").isNotNull() & F.col("fecha").isNotNull()))
    df = _dedup(df, ["ticker", "fecha"], "_fecha_ingesta")
    df = df.filter(F.col("cierre_ajustado") > 0)
    return (df.withColumn("calidad_ohlc_ok",
                          (F.col("maximo") >= F.greatest("apertura", "cierre")) &
                          (F.col("minimo") <= F.least("apertura", "cierre")))
              .drop("_fecha_ingesta"))


def agregar_rendimientos(precios: DataFrame) -> DataFrame:
    """Rendimiento diario simple y logarítmico sobre el cierre ajustado, por ticker."""
    w = Window.partitionBy("ticker").orderBy("fecha")
    previo = F.lag("cierre_ajustado").over(w)
    return (precios.withColumn("rendimiento_diario", F.col("cierre_ajustado") / previo - 1)
                   .withColumn("rendimiento_log", F.log(F.col("cierre_ajustado") / previo))
                   .withColumn("anio", F.year("fecha")))


def enriquecer_con_catalogo(precios: DataFrame, catalogo: DataFrame) -> DataFrame:
    cat = catalogo.select(F.upper("ticker").alias("ticker"), "nombre", "tipo", "sector", "industria")
    return precios.join(cat, "ticker", "left")

# ---------------------------------------------------------------- silver: dividendos y catálogo

def normalizar_dividendos(raw: DataFrame) -> DataFrame:
    df = raw.select(F.upper("ticker").alias("ticker"), F.to_date("fecha_pago").alias("fecha_pago"),
                    F.col("dividendo").cast("double").alias("dividendo"))
    return df.filter(F.col("dividendo") > 0).dropDuplicates(["ticker", "fecha_pago"])


def normalizar_catalogo(raw: DataFrame) -> DataFrame:
    return (raw.select(F.upper(F.trim("ticker")).alias("ticker"), F.trim("nombre").alias("nombre"), "tipo",
                       F.coalesce(F.col("sector"), F.lit("Sin sector")).alias("sector"),
                       F.coalesce(F.col("industria"), F.lit("")).alias("industria"),
                       F.col("capitalizacion_usd").cast("double").alias("capitalizacion_usd"))
               .dropDuplicates(["ticker"]))

# ---------------------------------------------------------------- silver: operaciones

def normalizar_operaciones(raw: DataFrame, mapa_futuros: DataFrame) -> DataFrame:
    """Operaciones con tipos correctos, dirección normalizada y su ETF subyacente (si existe)."""
    mapa = mapa_futuros.select(F.upper("simbolo_futuro").alias("simbolo"), "ticker_subyacente",
                               F.col("valor_punto_usd").cast("double").alias("valor_punto_usd"))
    df = (raw.select(
            F.col("id_operacion").cast("long").alias("id_operacion"),
            F.to_timestamp("entrada_utc").alias("entrada_utc"),
            F.to_timestamp("salida_utc").alias("salida_utc"),
            F.to_date("fecha").alias("fecha"),
            F.col("familia"), F.upper("simbolo").alias("simbolo"),
            F.when(F.lower("lado").isin("long", "buy", "largo"), "LARGO").otherwise("CORTO").alias("lado"),
            F.col("qty").cast("int").alias("contratos"),
            F.col("precio_entrada").cast("double"), F.col("precio_salida").cast("double"),
            F.col("puntos").cast("double"), F.col("usd_neto").cast("double"),
            F.col("mfe_ticks").cast("double"), F.col("mae_ticks").cast("double"),
            F.col("resultado"), F.col("duracion_min").cast("double"))
          .dropDuplicates(["id_operacion"]))
    return (df.join(F.broadcast(mapa), "simbolo", "left")
              .withColumn("gana", F.col("usd_neto") > 0)
              .withColumn("con_subyacente", F.col("ticker_subyacente").isNotNull()))


def regimen_vix(precios: DataFrame) -> DataFrame:
    """Régimen de volatilidad diario a partir del VIX: BAJO (<15), MEDIO (15–25), ALTO (>25)."""
    return (precios.filter(F.col("ticker") == "VIX")
                   .select("fecha", F.col("cierre").alias("vix_cierre"))
                   .withColumn("regimen_vix", F.when(F.col("vix_cierre") < 15, "BAJO")
                                               .when(F.col("vix_cierre") <= 25, "MEDIO").otherwise("ALTO")))


def contexto_de_mercado(operaciones: DataFrame, precios: DataFrame) -> DataFrame:
    """Une cada operación con el rendimiento de su subyacente ese día y el régimen de VIX."""
    # Los futuros operan el domingo por la tarde: esas operaciones se asignan a la sesión del viernes previo
    dow = F.dayofweek("fecha")  # 1 = domingo, 7 = sábado
    ops = operaciones.withColumn("fecha_mercado", F.when(dow == 1, F.date_sub("fecha", 2))
                                                   .when(dow == 7, F.date_sub("fecha", 1)).otherwise(F.col("fecha")))
    sub = precios.select(F.col("ticker").alias("ticker_subyacente"), F.col("fecha").alias("fecha_mercado"),
                         F.col("rendimiento_diario").alias("rendimiento_subyacente"))
    vix = regimen_vix(precios).withColumnRenamed("fecha", "fecha_mercado")
    return (ops.join(sub, ["ticker_subyacente", "fecha_mercado"], "left")
               .join(F.broadcast(vix), "fecha_mercado", "left"))

# ---------------------------------------------------------------- gold

def kpis_activo_anual(precios: DataFrame, dividendos: DataFrame) -> DataFrame:
    """Por ticker y año: rendimiento, volatilidad anualizada, máximo drawdown y rendimiento por dividendo."""
    w_ord = Window.partitionBy("ticker", "anio").orderBy("fecha")
    w_acum = w_ord.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    dd = (precios.withColumn("pico", F.max("cierre_ajustado").over(w_acum))
                 .withColumn("drawdown", F.col("cierre_ajustado") / F.col("pico") - 1))
    w_todo = Window.partitionBy("ticker", "anio")
    base = (dd.withColumn("primer", F.first("cierre_ajustado").over(w_ord.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
              .withColumn("ultimo", F.last("cierre_ajustado").over(w_ord.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
              .withColumn("ultimo_cierre", F.last("cierre").over(w_ord.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))))
    kpi = (base.groupBy("ticker", "anio")
               .agg(F.count("*").alias("sesiones"),
                    (F.first("ultimo") / F.first("primer") - 1).alias("rendimiento_anual"),
                    (F.stddev("rendimiento_diario") * F.sqrt(F.lit(252))).alias("volatilidad_anual"),
                    F.min("drawdown").alias("max_drawdown"),
                    F.first("ultimo_cierre").alias("cierre_fin_anio")))
    div = (dividendos.withColumn("anio", F.year("fecha_pago"))
                     .groupBy("ticker", "anio").agg(F.sum("dividendo").alias("dividendos_anio"), F.count("*").alias("pagos_anio")))
    return (kpi.join(div, ["ticker", "anio"], "left")
               .fillna({"dividendos_anio": 0.0, "pagos_anio": 0})
               .withColumn("rendimiento_dividendo", F.col("dividendos_anio") / F.col("cierre_fin_anio"))
               .withColumn("ratio_rend_riesgo", F.col("rendimiento_anual") / F.col("volatilidad_anual")))


def kpis_sector_anual(kpis_activo: DataFrame, catalogo: DataFrame) -> DataFrame:
    return (kpis_activo.join(catalogo.select("ticker", "sector", "tipo"), "ticker", "left")
                       .filter(F.col("tipo") != "Indice")
                       .groupBy("sector", "anio")
                       .agg(F.countDistinct("ticker").alias("activos"),
                            F.avg("rendimiento_anual").alias("rendimiento_promedio"),
                            F.avg("volatilidad_anual").alias("volatilidad_promedio"),
                            F.min("max_drawdown").alias("peor_drawdown"),
                            F.avg("rendimiento_dividendo").alias("rendimiento_dividendo_promedio")))


def kpis_estrategias(operaciones: DataFrame) -> DataFrame:
    """Por familia, mes y régimen de VIX: operaciones, acierto, profit factor y expectativa en USD."""
    ops = operaciones.withColumn("mes", F.date_format("fecha", "yyyy-MM"))
    g = (ops.groupBy("familia", "mes", "regimen_vix")
            .agg(F.count("*").alias("operaciones"),
                 F.avg(F.col("gana").cast("int")).alias("tasa_acierto"),
                 F.sum(F.when(F.col("usd_neto") > 0, F.col("usd_neto")).otherwise(0)).alias("ganancia_bruta"),
                 F.sum(F.when(F.col("usd_neto") < 0, -F.col("usd_neto")).otherwise(0)).alias("perdida_bruta"),
                 F.sum("usd_neto").alias("usd_neto"),
                 F.avg("usd_neto").alias("expectativa_usd")))
    return g.withColumn("profit_factor", F.when(F.col("perdida_bruta") > 0, F.col("ganancia_bruta") / F.col("perdida_bruta")))


def senales_estrategias_base(precios: DataFrame) -> DataFrame:
    """Señales diarias de las dos estrategias base del perfil de Oracle Investment.

    E1 seguimiento de tendencia: cierre > máximo de 55 sesiones previas y sobre la media de 200 (largo),
       o cierre < mínimo de 55 previas y bajo la media de 200 (corto).
    E2 reversión a la media: RSI(2) < 10 con el cierre sobre la media de 200 (compra de sobreventa).
       El RSI se calcula con medias simples de 2 sesiones (versión sin suavizado de Wilder).
    """
    w = Window.partitionBy("ticker").orderBy("fecha")
    p = (precios.withColumn("max55", F.max("maximo").over(w.rowsBetween(-55, -1)))
                .withColumn("min55", F.min("minimo").over(w.rowsBetween(-55, -1)))
                .withColumn("sma200", F.avg("cierre").over(w.rowsBetween(-199, 0)))
                .withColumn("n200", F.count("cierre").over(w.rowsBetween(-199, 0)))
                .withColumn("delta", F.col("cierre") - F.lag("cierre").over(w))
                .withColumn("subida", F.greatest(F.col("delta"), F.lit(0.0)))
                .withColumn("bajada", F.greatest(-F.col("delta"), F.lit(0.0)))
                .withColumn("ganancia2", F.avg("subida").over(w.rowsBetween(-1, 0)))
                .withColumn("perdida2", F.avg("bajada").over(w.rowsBetween(-1, 0))))
    p = p.withColumn("rsi2", F.when(F.col("perdida2") == 0, F.lit(100.0))
                               .otherwise(100 - 100 / (1 + F.col("ganancia2") / F.col("perdida2"))))
    valido = F.col("n200") >= 200
    return (p.withColumn("senal_e1", F.when(valido & (F.col("cierre") > F.col("max55")) & (F.col("cierre") > F.col("sma200")), "LARGO")
                                       .when(valido & (F.col("cierre") < F.col("min55")) & (F.col("cierre") < F.col("sma200")), "CORTO"))
             .withColumn("senal_e2", F.when(valido & (F.col("rsi2") < 10) & (F.col("cierre") > F.col("sma200")), "COMPRA"))
             .select("ticker", "fecha", "cierre", "sma200", "max55", "min55", "rsi2", "senal_e1", "senal_e2"))
