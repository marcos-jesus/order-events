#!/usr/bin/env python3
"""Gera os dashboards do Grafana (datasource Zabbix). Uso: python3 gen_dashboards.py"""
import json
import pathlib

OUT = pathlib.Path(__file__).parent / "dashboards"
DS = {"type": "alexanderzobnin-zabbix-datasource", "uid": "zabbix"}
GROUP = "Order Events"


def target(ref, host, item):
    return {
        "datasource": DS, "refId": ref, "queryType": "0",
        "group": {"filter": GROUP}, "host": {"filter": host},
        "application": {"filter": ""}, "itemTag": {"filter": ""},
        "item": {"filter": item}, "functions": [],
        "options": {"showDisabledItems": False, "skipEmptyValues": False,
                    "disableDataAlignment": False, "useZabbixValueMapping": False},
    }


def panel(pid, title, host, items, x, y, w=12, h=8, unit="short", ptype="timeseries"):
    targets = [target(chr(65 + i), host, item) for i, item in enumerate(items)]
    return {
        "id": pid, "type": ptype, "title": title, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "targets": targets,
    }


def dashboard(uid, title, panels):
    return {
        "uid": uid, "title": title, "tags": ["order-events"], "schemaVersion": 39,
        "version": 1, "editable": False, "refresh": "30s",
        "time": {"from": "now-1h", "to": "now"}, "panels": panels,
    }


APP = "order-events-app"
DB = "order-events-postgres"

health_panel = panel(1, "Saúde da aplicação", APP, ["App: up"], 0, 0, w=6, h=4, ptype="stat")
health_panel["fieldConfig"]["defaults"].update({
    "noValue": "DOWN",
    "mappings": [{"type": "value", "options": {
        "1": {"text": "UP", "color": "green", "index": 0},
        "0": {"text": "DOWN", "color": "red", "index": 1}}}],
    "thresholds": {"mode": "absolute", "steps": [{"color": "red", "value": None}, {"color": "green", "value": 1}]},
})
health_panel["options"] = {"colorMode": "background", "reduceOptions": {"calcs": ["lastNotNull"]}}

app = dashboard("order-events-app", "Order Events — Aplicação", [
    health_panel,
    panel(2, "Mensagens na DLT (total)", APP, ["DLT: messages total"], 6, 0, w=6, h=4, ptype="stat"),
    panel(3, "Requests/s", APP, ["HTTP: requests/s"], 0, 4, unit="reqps"),
    panel(4, "Erros HTTP/s (4xx e 5xx)", APP, ["HTTP: 4xx/s", "HTTP: 5xx/s"], 12, 4, unit="reqps"),
    panel(5, "Latência HTTP (média e máxima)", APP, ["HTTP: avg latency", "HTTP: max latency"], 0, 12, unit="s"),
    panel(6, "Itens processados/s por consumer", APP,
          ["/Listener .*: processed\\/s/", "Billing: persisted/s"], 12, 12, unit="ops"),
    panel(7, "Falhas/s por consumer", APP, ["/Listener .*: failed\\/s/"], 0, 20, unit="ops"),
    panel(8, "Lag por consumer", APP, ["/Consumer .*: lag max/"], 12, 20),
    panel(9, "Heap JVM e threads", APP, ["JVM: heap used"], 0, 28, unit="bytes"),
    panel(10, "Pool Hikari (ativas vs máximo)", APP,
          ["Hikari: active connections", "Hikari: max connections"], 12, 28),
    panel(11, "Faturamento: tamanho do lote (máx.)", APP, ["Billing: batch size max"], 0, 36),
])

db = dashboard("order-events-db", "Order Events — Banco", [
    panel(1, "Postgres ping", DB, ["PG: ping"], 0, 0, w=6, h=4, ptype="stat"),
    panel(2, "Linhas em billing_record", DB, ["PG: billing_record rows"], 6, 0, w=6, h=4, ptype="stat"),
    panel(3, "Conexões", DB, ["PG: connections", "PG: connections %"], 0, 4),
    panel(4, "Transações/s", DB, ["PG: commits/s", "PG: rollbacks/s"], 12, 4, unit="ops"),
    panel(5, "Tamanho do banco e da tabela", DB,
          ["PG: database size", "PG: billing_record size"], 0, 12, unit="bytes"),
    panel(6, "Locks", DB, ["PG: locks"], 12, 12),
])

OUT.mkdir(exist_ok=True)
for name, dash in (("aplicacao.json", app), ("banco.json", db)):
    (OUT / name).write_text(json.dumps(dash, indent=2, ensure_ascii=False) + "\n")
    print("gerado", OUT / name)
