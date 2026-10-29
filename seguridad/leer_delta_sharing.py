# -*- coding: utf-8 -*-
"""Lectura del share desde fuera de Databricks (destinatario abierto).
pip install delta-sharing ; colocar el archivo de credenciales descargado del enlace de activación como config.share"""
import delta_sharing

perfil = "config.share"
cliente = delta_sharing.SharingClient(perfil)
for t in cliente.list_all_tables():
    print(t)
df = delta_sharing.load_as_pandas(f"{perfil}#oracle_kpis_mercado.gold.kpis_sector_anual")
print(df.sort_values(["anio", "rendimiento_promedio"], ascending=[False, False]).head(10))
