#!/bin/sh
# Prepara o Zabbix para o order-events: importa templates, cria grupo/hosts e o usuário
# somente leitura do Grafana. Pode rodar várias vezes sem duplicar nada.
set -eu

API="${ZBX_URL:-http://zabbix-web:8080/api_jsonrpc.php}"
ADMIN_USER="Admin"
ADMIN_PASS="zabbix"
GRAFANA_PASS="Gr4f-ro-Zbx-2026"
TOKEN=""

call() { # $1 = método, $2 = params (JSON)
  body=$(jq -n --arg m "$1" --argjson p "$2" '{jsonrpc:"2.0",method:$m,params:$p,id:1}')
  if [ -n "$TOKEN" ]; then
    resp=$(curl -fsS -H 'Content-Type: application/json-rpc' -H "Authorization: Bearer $TOKEN" -d "$body" "$API")
  else
    resp=$(curl -fsS -H 'Content-Type: application/json-rpc' -d "$body" "$API")
  fi
  if echo "$resp" | jq -e '.error' >/dev/null; then
    echo "erro na API do Zabbix em $1: $(echo "$resp" | jq -c '.error')" >&2
    exit 1
  fi
  echo "$resp" | jq -c '.result'
}

echo "Aguardando a API do Zabbix..."
i=0
until curl -fsS -H 'Content-Type: application/json-rpc' \
  -d '{"jsonrpc":"2.0","method":"apiinfo.version","params":[],"id":1}' "$API" >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -gt 90 ] && { echo "API do Zabbix indisponível" >&2; exit 1; }
  sleep 2
done

TOKEN=$(call user.login "$(jq -n --arg u "$ADMIN_USER" --arg p "$ADMIN_PASS" '{username:$u,password:$p}')" | jq -r .)

echo "Importando templates..."
RULES='{"template_groups":{"createMissing":true,"updateExisting":true},
        "templates":{"createMissing":true,"updateExisting":true},
        "items":{"createMissing":true,"updateExisting":true,"deleteMissing":true},
        "discoveryRules":{"createMissing":true,"updateExisting":true,"deleteMissing":true},
        "triggers":{"createMissing":true,"updateExisting":true,"deleteMissing":true}}'
for f in /zabbix/templates/*.yaml; do
  params=$(jq -n --arg s "$(cat "$f")" --argjson r "$RULES" '{format:"yaml",source:$s,rules:$r}')
  call configuration.import "$params" >/dev/null
  echo "  importado: $f"
done

GID=$(call hostgroup.get '{"filter":{"name":["Order Events"]}}' | jq -r '.[0].groupid // empty')
[ -n "$GID" ] || GID=$(call hostgroup.create '{"name":"Order Events"}' | jq -r '.groupids[0]')

tpl_id() {
  call template.get "$(jq -n --arg t "$1" '{filter:{host:[$t]}}')" | jq -r '.[0].templateid'
}

ensure_host() { # $1 = nome, $2 = template, $3 = interfaces (JSON)
  hid=$(call host.get "$(jq -n --arg n "$1" '{filter:{host:[$n]}}')" | jq -r '.[0].hostid // empty')
  if [ -z "$hid" ]; then
    call host.create "$(jq -n --arg n "$1" --arg g "$GID" --arg t "$(tpl_id "$2")" --argjson i "$3" \
      '{host:$n,groups:[{groupid:$g}],templates:[{templateid:$t}],interfaces:$i}')" >/dev/null
    echo "  host criado: $1"
  else
    echo "  host já existe: $1"
  fi
}

ensure_host order-events-app "Order Events App" '[]'
ensure_host order-events-postgres "Order Events Postgres" \
  '[{"type":1,"main":1,"useip":0,"ip":"","dns":"zabbix-agent2","port":"10050"}]'

UGID=$(call usergroup.get '{"filter":{"name":["grafana-readers"]}}' | jq -r '.[0].usrgrpid // empty')
if [ -z "$UGID" ]; then
  UGID=$(call usergroup.create "$(jq -n --arg g "$GID" \
    '{name:"grafana-readers",hostgroup_rights:[{id:$g,permission:2}]}')" | jq -r '.usrgrpids[0]')
fi
GUID=$(call user.get '{"filter":{"username":["grafana"]}}' | jq -r '.[0].userid // empty')
if [ -z "$GUID" ]; then
  call user.create "$(jq -n --arg g "$UGID" --arg p "$GRAFANA_PASS" \
    '{username:"grafana",passwd:$p,roleid:"1",usrgrps:[{usrgrpid:$g}]}')" >/dev/null
  echo "  usuário criado: grafana"
fi

echo "bootstrap OK"
