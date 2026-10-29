# -*- coding: utf-8 -*-
"""Databricks App · Portal de datos de Oracle Investment.

Pestaña 1: panel de KPIs y señales vigentes (lee gold con el almacén SQL).
Pestaña 2: autoservicio de acceso: el usuario pide permiso de lectura a una tabla gold; la solicitud queda en
           control.solicitudes_acceso para que ingeniería la apruebe (no concede permisos por sí sola).
"""
import os
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

CATALOGO = os.getenv("CATALOGO", "oracle_prd")
ALMACEN = os.getenv("DATABRICKS_WAREHOUSE_ID")
cfg = Config()  # la app hereda la identidad de su service principal


def consulta(q: str, params=None) -> pd.DataFrame:
    with sql.connect(server_hostname=cfg.host, http_path=f"/sql/1.0/warehouses/{ALMACEN}",
                     credentials_provider=lambda: cfg.authenticate) as con:
        with con.cursor() as cur:
            cur.execute(q, params or {})
            return cur.fetchall_arrow().to_pandas()


st.set_page_config(page_title="Oracle Investment · Datos", layout="wide")
st.title("Oracle Investment · Portal de datos")
panel, acceso = st.tabs(["Panel de mercado", "Solicitar acceso"])

with panel:
    anio = st.selectbox("Año", list(range(2026, 2014, -1)))
    sec = consulta(f"SELECT sector, rendimiento_promedio, volatilidad_promedio FROM {CATALOGO}.gold.kpis_sector_anual "
                   "WHERE anio = :anio ORDER BY rendimiento_promedio DESC", {"anio": anio})
    c1, c2 = st.columns(2)
    c1.subheader("Rendimiento promedio por sector")
    c1.bar_chart(sec.set_index("sector")["rendimiento_promedio"])
    c2.subheader("Señales vigentes de las estrategias base")
    c2.dataframe(consulta(f"SELECT ticker, sector, cierre, rsi2, senal_e1, senal_e2 FROM {CATALOGO}.gold.panel_activos "
                          "WHERE senal_e1 IS NOT NULL OR senal_e2 IS NOT NULL ORDER BY ticker"), use_container_width=True)

with acceso:
    tablas = consulta(f"SELECT table_name FROM {CATALOGO}.information_schema.tables WHERE table_schema = 'gold' ORDER BY 1")
    with st.form("solicitud"):
        correo = st.text_input("Correo corporativo")
        tabla = st.selectbox("Tabla gold", tablas["table_name"].tolist())
        motivo = st.text_area("Para qué la necesitas")
        if st.form_submit_button("Enviar solicitud") and correo and motivo:
            consulta(f"INSERT INTO {CATALOGO}.control.solicitudes_acceso VALUES (current_timestamp(), :c, :t, :m, 'PENDIENTE')",
                     {"c": correo, "t": tabla, "m": motivo})
            st.success("Solicitud registrada. Ingeniería de datos la revisará y aplicará el GRANT si corresponde.")
