# -*- coding: utf-8 -*-
"""Dibuja el diagrama de arquitectura. Uso: python herramientas/dibujar_arquitectura.py evidencias/00_arquitectura.png"""
import sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

TINTA, GRIS, LINEA = "#1F2937", "#6B7280", "#CBD5E1"
ROJO, AZUL, VERDE, AMBAR, MORADO = "#E4572E", "#2563EB", "#15803D", "#B45309", "#6D28D9"
fig, ax = plt.subplots(figsize=(16, 9), dpi=150)
ax.set_xlim(0, 16); ax.set_ylim(0, 9); ax.axis("off")


def caja(x, y, w, h, titulo, lineas=(), color=TINTA, fondo="#FFFFFF", borde=None, tam=11):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.12",
                                fc=fondo, ec=borde or color, lw=1.6))
    ax.text(x + w / 2, y + h - 0.28, titulo, ha="center", va="center", fontsize=tam, fontweight="bold", color=color)
    for i, l in enumerate(lineas):
        ax.text(x + w / 2, y + h - 0.62 - i * 0.27, l, ha="center", va="center", fontsize=8.6, color=TINTA)


def marco(x, y, w, h, titulo, color=GRIS):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.15",
                                fc="#F8FAFC", ec=LINEA, lw=1.2))
    ax.text(x + 0.18, y + h - 0.25, titulo, ha="left", va="center", fontsize=10, fontweight="bold", color=color)


def flecha(x1, y1, x2, y2, color=GRIS, estilo="-", texto=None, dy=0.16):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=14, lw=1.6,
                                 color=color, linestyle=estilo))
    if texto:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + dy, texto, ha="center", va="center", fontsize=8.2, color=color)


ax.text(0.3, 8.62, "Oracle Lakehouse · Arquitectura medallion en Azure Databricks", fontsize=17, fontweight="bold", color=TINTA)
ax.text(0.3, 8.25, "Raw externo en ADLS Gen2 con Managed Identity · ETL en PySpark · Unity Catalog · CI/CD con GitHub Actions y Asset Bundles",
        fontsize=10.5, color=GRIS)

# Fuentes
marco(0.3, 2.2, 2.5, 5.6, "Fuentes")
caja(0.5, 6.1, 2.1, 1.2, "Yahoo Finance", ["precios (CSV por año)", "dividendos (JSON)"], AZUL)
caja(0.5, 4.5, 2.1, 1.2, "Bitácora", ["operaciones NinjaTrader", "SQLite → Parquet"], AZUL)
caja(0.5, 2.9, 2.1, 1.2, "Referencia", ["catálogo de activos (CSV)", "mapa de futuros (CSV)"], AZUL)

# ADLS
marco(3.3, 2.2, 2.3, 5.6, "ADLS Gen2")
caja(3.5, 5.6, 1.9, 1.7, "raw", ["precios/", "dividendos/", "catalogo/", "operaciones/"], AMBAR, "#FFFBEB")
caja(3.5, 2.5, 1.9, 2.7, "Managed Identity", ["Access Connector", "↓", "storage credential", "↓", "external location", "por contenedor"], VERDE, "#F0FDF4", tam=10)
flecha(2.6, 6.7, 3.5, 6.6); flecha(2.6, 5.1, 3.5, 6.2); flecha(2.6, 3.5, 3.5, 5.9)

# Databricks
marco(6.1, 2.2, 6.2, 5.6, "Azure Databricks · Unity Catalog · oracle_dev / oracle_prd", ROJO)
caja(6.3, 5.4, 1.75, 1.75, "BRONZE", ["Extract", "ingesta incremental", "idempotente", "5 tablas externas"], "#92400E", "#FEF3C7")
caja(8.3, 5.4, 1.75, 1.75, "SILVER", ["Transform", "tipado · joins", "MERGE (Delta)", "4 tablas externas"], "#475569", "#F1F5F9")
caja(10.3, 5.4, 1.8, 1.75, "GOLD", ["Load", "KPIs y señales", "filtro y máscara", "5 tablas externas"], "#A16207", "#FEF9C3")
flecha(5.4, 6.45, 6.3, 6.3, AMBAR); flecha(8.05, 6.28, 8.3, 6.28); flecha(10.05, 6.28, 10.3, 6.28)
caja(6.3, 3.7, 5.8, 1.35, "Workflow (Job) · un clúster efímero de un nodo",
     ["preparación → extract ×5 en paralelo → transform ×2 → load ×4 → grants",
      "reintentos · 7 reglas de calidad · mantenimiento Delta (time travel, OPTIMIZE, VACUUM)"], ROJO, "#FEF2F2", tam=10)
caja(6.3, 2.4, 2.8, 1.05, "control", ["ejecuciones · calidad"], GRIS, tam=10)
caja(9.3, 2.4, 2.8, 1.05, "Seguridad", ["grupos · grants · Delta Sharing"], MORADO, tam=10)
for x in (7.2, 9.2, 11.2):
    flecha(x, 5.0, x, 5.4, ROJO, ":")

# Consumo
marco(12.8, 2.2, 2.9, 5.6, "Consumo")
caja(13.0, 6.35, 2.5, 0.95, "Azure SQL Database", ["base transaccional (JDBC)"], AZUL, tam=10)
caja(13.0, 5.25, 2.5, 0.95, "Tablero AI/BI (Lakeview)", ["KPIs y salud del pipeline"], ROJO, tam=10)
caja(13.0, 4.15, 2.5, 0.95, "Genie", ["preguntas en lenguaje natural"], ROJO, tam=10)
caja(13.0, 3.05, 2.5, 0.95, "Databricks App", ["portal Streamlit"], ROJO, tam=10)
caja(13.0, 2.35, 2.5, 0.55, "Delta Sharing", [], MORADO, tam=9)
for y in (6.8, 5.7, 4.6, 3.5, 2.62):
    flecha(12.1, 6.2 if y > 5 else 5.7, 13.0, y, GRIS)

# CI/CD
marco(0.3, 0.3, 15.4, 1.6, "CI/CD", TINTA)
pasos = [("GitHub · push a main", TINTA), ("Pruebas unitarias\n(pytest + Spark local)", VERDE), ("bundle validate", AZUL),
         ("bundle deploy → prod\n(proceso/ + workflow)", ROJO), ("bundle run\n(ejecuta el workflow)", ROJO),
         ("Resumen por tarea\nen GitHub Actions", TINTA)]
x = 1.4
for i, (t, c) in enumerate(pasos):
    ax.add_patch(FancyBboxPatch((x, 0.5), 2.05, 0.85, boxstyle="round,pad=0.02,rounding_size=0.1", fc="#FFFFFF", ec=c, lw=1.5))
    ax.text(x + 1.025, 0.925, t, ha="center", va="center", fontsize=8.3, color=c, fontweight="bold")
    if i < len(pasos) - 1:
        flecha(x + 2.07, 0.925, x + 2.38, 0.925, GRIS)
    x += 2.4
flecha(9.2, 1.9, 9.2, 3.7, ROJO, "--")

fig.savefig(sys.argv[1], bbox_inches="tight", facecolor="white")
print("ok", sys.argv[1])
