# -*- coding: utf-8 -*-
"""Genera los insumos RAW del proyecto (capa raw en ADLS).

Fuentes y formatos (distintos a propósito, como en un data lake real):
  precios/precios_diarios_<AAAA>.csv   OHLCV diario de 30 activos + VIX y tasa a 10 años (Yahoo Finance)
  dividendos/dividendos.json           pagos de dividendos, JSON por línea (Yahoo Finance)
  catalogo/catalogo_activos.csv        ticker, nombre, tipo, sector, industria, capitalización
  catalogo/mapa_futuros.csv            micro futuro CME -> ETF subyacente (tabla de referencia)
  operaciones/operaciones.parquet      bitácora de operaciones de Oracle Investment (simulación), anonimizada

Uso:  python descargar_insumos.py --salida ./datos   (luego se sube a ADLS con subir_a_adls.md)
Requiere: yfinance, pandas, pyarrow. Si hay un proxy TLS corporativo, definir CURL_CA_BUNDLE.
"""
import argparse, json, os, sqlite3
from pathlib import Path
import pandas as pd
import yfinance as yf

ACCIONES = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "JPM", "XOM", "JNJ", "KO", "PG", "HD", "CVX", "PEP", "T"]
ETFS = ["SPY", "QQQ", "DIA", "IWM", "GLD", "TLT", "USO", "XLF", "XLK", "XLE", "XLV", "XLU", "XLP", "XLY", "XLI", "SLV", "FXE", "FXB"]
INDICES = {"^VIX": "VIX", "^TNX": "TNX"}
INICIO, FIN = "2015-01-01", "2026-10-29"

MAPA_FUTUROS = [  # micro futuro, nombre, subyacente ETF, valor del punto USD
    ("MES", "Micro E-mini S&P 500", "SPY", 5.0), ("ES", "E-mini S&P 500", "SPY", 50.0),
    ("MNQ", "Micro E-mini Nasdaq-100", "QQQ", 2.0), ("NQ", "E-mini Nasdaq-100", "QQQ", 20.0),
    ("MYM", "Micro E-mini Dow", "DIA", 0.5), ("YM", "E-mini Dow", "DIA", 5.0),
    ("M2K", "Micro E-mini Russell 2000", "IWM", 5.0), ("RTY", "E-mini Russell 2000", "IWM", 50.0),
    ("MGC", "Micro Oro", "GLD", 10.0), ("GC", "Oro", "GLD", 100.0),
    ("MCL", "Micro Petróleo WTI", "USO", 100.0), ("CL", "Petróleo WTI", "USO", 1000.0),
    ("SIL", "Micro Plata", "SLV", 1000.0), ("SI", "Plata", "SLV", 5000.0),
    ("6E", "Euro FX", "FXE", 125000.0), ("M6E", "Micro Euro FX", "FXE", 12500.0),
    ("6B", "Libra esterlina", "FXB", 62500.0), ("M6B", "Micro Libra", "FXB", 6250.0),
]

def precios(salida: Path):
    tick = ACCIONES + ETFS + list(INDICES)
    df = yf.download(tick, start=INICIO, end=FIN, auto_adjust=False, progress=False, group_by="ticker")
    filas = []
    for t in tick:
        if t not in df.columns.get_level_values(0): continue
        d = df[t].dropna(how="all").reset_index()
        d["ticker"] = INDICES.get(t, t)
        filas.append(d)
    p = pd.concat(filas).rename(columns={"Date": "fecha", "Open": "apertura", "High": "maximo", "Low": "minimo",
                                         "Close": "cierre", "Adj Close": "cierre_ajustado", "Volume": "volumen"})
    p["fecha"] = pd.to_datetime(p["fecha"]).dt.strftime("%Y-%m-%d")
    out = salida / "precios"; out.mkdir(parents=True, exist_ok=True)
    for anio, g in p.groupby(p["fecha"].str[:4]):
        g[["fecha", "ticker", "apertura", "maximo", "minimo", "cierre", "cierre_ajustado", "volumen"]].to_csv(
            out / f"precios_diarios_{anio}.csv", index=False)
    print("precios:", len(p), "filas,", p["ticker"].nunique(), "tickers")

def dividendos_y_catalogo(salida: Path):
    (salida / "dividendos").mkdir(parents=True, exist_ok=True)
    (salida / "catalogo").mkdir(parents=True, exist_ok=True)
    cat, n = [], 0
    with open(salida / "dividendos" / "dividendos.json", "w", encoding="utf-8") as fj:
        for t in ACCIONES + ETFS:
            tk = yf.Ticker(t)
            dv = tk.dividends
            for f, v in dv.items():
                if str(f)[:10] >= INICIO:
                    fj.write(json.dumps({"ticker": t, "fecha_pago": str(f)[:10], "dividendo": round(float(v), 6)}) + "\n"); n += 1
            info = {}
            try: info = tk.info or {}
            except Exception: pass
            cat.append({"ticker": t, "nombre": info.get("longName") or info.get("shortName") or t,
                        "tipo": "ETF" if t in ETFS else "Accion",
                        "sector": info.get("sector") or ("ETF " + (info.get("category") or "indice")),
                        "industria": info.get("industry") or info.get("category") or "",
                        "capitalizacion_usd": info.get("marketCap") or info.get("totalAssets"),
                        "moneda": info.get("currency") or "USD"})
    for k, v in INDICES.items():
        cat.append({"ticker": v, "nombre": k, "tipo": "Indice", "sector": "Macro", "industria": "Volatilidad" if v == "VIX" else "Tasas",
                    "capitalizacion_usd": None, "moneda": "USD"})
    pd.DataFrame(cat).to_csv(salida / "catalogo" / "catalogo_activos.csv", index=False)
    pd.DataFrame(MAPA_FUTUROS, columns=["simbolo_futuro", "nombre_futuro", "ticker_subyacente", "valor_punto_usd"]).to_csv(
        salida / "catalogo" / "mapa_futuros.csv", index=False)
    print("dividendos:", n, "| catalogo:", len(cat))

FAMILIAS = {  # anonimización: las familias se publican con código genérico
    "POC_FADE_V2": "F1", "NinjaBot_Range": "F2", "POC_Suite": "F3", "NinjaBot_Trend": "F4", "NinjaBot_TakeProfit": "F5",
    "PV_Barrido": "F6", "MetodoIFT": "F7"}

def operaciones(salida: Path, bitacora: Path):
    if not bitacora.exists():
        print("operaciones: bitácora no encontrada, se omite"); return
    c = sqlite3.connect(f"file:{bitacora}?mode=ro", uri=True)
    q = """select t.id as id_operacion, t.entrada_utc, t.salida_utc, t.fecha, t.familia, t.simbolo, t.lado, t.qty,
                  t.precio_entrada, t.precio_salida, t.puntos, t.usd_neto, t.mfe_ticks, t.mae_ticks, t.resultado, t.duracion_min
           from trades t where t.entorno = 'Sim'"""
    o = pd.read_sql(q, c)
    o["familia"] = o["familia"].map(lambda f: FAMILIAS.get(f, "F0"))  # F0 = otras familias
    o["id_operacion"] = range(1, len(o) + 1)
    o["simbolo"] = o["simbolo"].str.upper().str.extract(r"^([A-Z0-9]+)")[0]
    out = salida / "operaciones"; out.mkdir(parents=True, exist_ok=True)
    o.to_parquet(out / "operaciones.parquet", index=False)
    print("operaciones:", len(o), "filas")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--salida", default=str(Path(__file__).with_name("datos")))
    ap.add_argument("--bitacora", default=r"C:\Users\Windows 11\Documents\Proyectos\Oracle Investment\Oracle_Bitacora\datos\oracle_bitacora.sqlite")
    a = ap.parse_args()
    s = Path(a.salida); s.mkdir(parents=True, exist_ok=True)
    precios(s); dividendos_y_catalogo(s); operaciones(s, Path(a.bitacora))
