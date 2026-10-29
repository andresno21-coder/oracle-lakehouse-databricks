-- Tabla que alimenta el autoservicio de accesos de la app (la app solo inserta; ingeniería aprueba)
CREATE TABLE IF NOT EXISTS oracle_prd.control.solicitudes_acceso (
  fecha_utc TIMESTAMP, correo STRING, tabla STRING, motivo STRING, estado STRING
);
-- El service principal de la app solo puede leer gold e insertar solicitudes
GRANT USE CATALOG ON CATALOG oracle_prd TO `sp-oracle-app`;
GRANT USE SCHEMA, SELECT ON SCHEMA oracle_prd.gold TO `sp-oracle-app`;
GRANT USE SCHEMA ON SCHEMA oracle_prd.control TO `sp-oracle-app`;
GRANT MODIFY, SELECT ON TABLE oracle_prd.control.solicitudes_acceso TO `sp-oracle-app`;
