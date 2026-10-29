# Crea los usuarios ficticios y los grupos DE CUENTA del proyecto (Unity Catalog no reconoce grupos locales del workspace).
# Usa la API SCIM de cuenta expuesta por el workspace, así basta con ser administrador del workspace.
# Uso:  pwsh herramientas/crear_usuarios_y_grupos.ps1 -Perfil oracle-dev
param([string]$Perfil = 'DEFAULT')

$plan = [ordered]@{
  'oi_ingenieros_datos' = 'ana.ingeniera@oracleinvestment.example'
  'oi_analistas_riesgo' = 'luis.riesgo@oracleinvestment.example'
  'oi_consumidores_bi'  = 'marta.bi@oracleinvestment.example'
  'oi_auditores'        = 'jorge.auditor@oracleinvestment.example'
}

function Api($metodo, $ruta, $cuerpo) {
  if ($cuerpo) {
    $tmp = New-TemporaryFile
    [IO.File]::WriteAllText($tmp, ($cuerpo | ConvertTo-Json -Depth 6 -Compress))
    $r = databricks api $metodo $ruta --json "@$tmp" -p $Perfil | Out-String
    Remove-Item $tmp
  } else { $r = databricks api $metodo $ruta -p $Perfil | Out-String }
  if ($r.Trim().StartsWith('{')) { $r | ConvertFrom-Json } else { $null }
}

$usuarios = (Api get '/api/2.0/account/scim/v2/Users?count=200').Resources
$grupos = (Api get '/api/2.0/account/scim/v2/Groups?count=200').Resources
foreach ($g in $plan.Keys) {
  $correo = $plan[$g]
  $u = $usuarios | Where-Object userName -eq $correo
  if (-not $u) {
    # Agregar el usuario al workspace lo crea también en la cuenta
    databricks users create --user-name $correo --display-name ($correo.Split('@')[0]) -p $Perfil | Out-Null
    $u = (Api get "/api/2.0/account/scim/v2/Users?filter=userName%20eq%20%22$correo%22").Resources | Select-Object -First 1
  }
  if (-not ($grupos | Where-Object displayName -eq $g)) {
    $cuerpo = @{ schemas = @('urn:ietf:params:scim:schemas:core:2.0:Group'); displayName = $g; members = @(@{ value = $u.id }) }
    $nuevo = Api post '/api/2.0/account/scim/v2/Groups' $cuerpo
    "{0,-22} creado ({1}) con {2}" -f $g, $nuevo.id, $correo
  } else { "{0,-22} ya existe" -f $g }
}
