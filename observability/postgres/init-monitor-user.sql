-- Usuário somente leitura para o Zabbix Agent2 (grupo pg_monitor); pode rodar várias vezes.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'zbx_monitor') THEN
        CREATE ROLE zbx_monitor LOGIN PASSWORD 'zbx_monitor';
    END IF;
END
$$;

GRANT pg_monitor TO zbx_monitor;
GRANT CONNECT ON DATABASE order_events TO zbx_monitor;
