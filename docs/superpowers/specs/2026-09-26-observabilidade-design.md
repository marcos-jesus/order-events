# Observabilidade com Zabbix e Grafana — Design

Data: 2026-09-26

## Objetivo

Visualizar em dashboards do Grafana a saúde e a vazão do `order-events`: requests HTTP,
itens processados pelos consumers Kafka (incluindo lag), e o banco de dados Postgres.
O Zabbix é a fonte única de coleta, armazenamento e alertas; o Grafana apenas exibe,
via plugin de datasource do Zabbix.

## Decisões

- **Abordagem A:** o Zabbix lê o endpoint Prometheus da app (`/actuator/prometheus`) com item
  HTTP agent + pré-processamento Prometheus nativo do Zabbix 7 e regras LLD. Sem Prometheus,
  sem agent dentro da app, sem código de integração.
- **Kafka:** apenas métricas expostas pela própria app (`kafka.consumer.*`, lag por consumer
  group). Sem exporter/JMX do broker.
- **Stack separada** em `docker-compose.observability.yml`; o `docker-compose.yml` básico
  não muda. Os `docker-compose.cluster*.yml` também não são tocados.
- **Fora de escopo:** logs, tracing, métricas do broker Kafka, containerização da app.

## 1. Instrumentação da app

Dependências: `spring-boot-starter-actuator` e `micrometer-registry-prometheus`.

`application.yml`:
- `management.endpoints.web.exposure.include: health,info,prometheus`
- `management.metrics.tags.application: order-events`
- `spring.kafka.listener.observation-enabled: true` (timer `spring.kafka.listener` por listener,
  com contagem de sucesso/erro)

Métricas disponíveis:

| Necessidade | Métrica | Origem |
|---|---|---|
| Requests, latência, status | `http.server.requests` | Actuator |
| Pool do banco | `hikaricp.connections.*` | Actuator |
| Lag e consumo Kafka | `kafka.consumer.*` (`records-lag-max`, `records-consumed-total`) | Micrometer Kafka |
| Itens processados por consumer | `spring.kafka.listener` (tag por listener, resultado) | observation |
| Mensagens na DLT | `orders.dead_lettered` (counter, novo) | código |
| JVM | `jvm.*`, `process.*` | Actuator |

Mudanças de código:
- Dar `id` explícito aos três `@KafkaListener` (`inventory`, `notification`, `billing`) para
  que a tag do timer identifique o consumer group de forma estável.
- Incrementar o counter `orders.dead_lettered` no recoverer da DLT em `KafkaConsumerConfig`.

A app continua rodando no host (`./mvnw spring-boot:run`, porta 8080); o Zabbix a acessa
por `host.docker.internal` (com `extra_hosts: host-gateway` no compose, necessário em Linux).

## 2. Stack (`docker-compose.observability.yml`)

Serviços, todos com versões fixas (sem `latest`):
- `zabbix-db`: Postgres exclusivo do Zabbix, volume próprio.
- `zabbix-server`, `zabbix-web` (porta 8083).
- `zabbix-agent2`: plugin PostgreSQL apontando para o Postgres da app (usuário somente
  leitura, criado por script de init do compose).
- `grafana` (porta 3000) com o plugin `alexanderzobnin-zabbix-app` instalado via
  `GF_INSTALL_PLUGINS` e habilitado por provisionamento.
- `zabbix-bootstrap`: container efêmero que, via API do Zabbix, importa os templates,
  cria os hosts (app e banco) e o token/usuário de leitura usado pelo Grafana; idempotente.

Redes: as duas stacks precisam se enxergar (Postgres da app). A observability se conecta à
rede default do compose principal (rede externa nomeada) para alcançar `postgres`.

## 3. Templates Zabbix e Grafana (arquivos versionados em `observability/`)

```
observability/
  zabbix/templates/order-events-app.yaml
  zabbix/templates/order-events-postgres.yaml
  zabbix/bootstrap.sh
  postgres/init-monitor-user.sql
  grafana/provisioning/datasources/zabbix.yaml
  grafana/provisioning/dashboards/dashboards.yaml
  grafana/dashboards/aplicacao.json
  grafana/dashboards/banco.json
```

Template da app: item mestre HTTP agent (`/actuator/prometheus`, texto bruto) e itens
dependentes com `Prometheus pattern`; LLD via `Prometheus to JSON` para métricas por
consumer/endpoint/status.

Dashboard **Aplicação**: requests/s, latência média e máxima, taxa de erro 4xx/5xx, itens
processados por consumer, lag por consumer group, DLT, heap e threads.
Dashboard **Banco**: conexões ativas, transações/s, tamanho do banco e da tabela
`billing_record`, locks, uso do pool Hikari.

## 4. Alertas (triggers no Zabbix)

- App fora do ar (item mestre sem dados / `health` != UP).
- Lag de consumer acima do limite por 5 min.
- Taxa de erro HTTP 5xx acima do limite.
- Pool Hikari saturado.
- Qualquer incremento de `orders.dead_lettered`.
- Postgres inacessível ou conexões próximas do máximo.

Limites são macros do template, ajustáveis sem editar triggers. Sem canal de notificação
configurado (só visualização na UI do Zabbix), por ser ambiente local.

## 5. Testes e validação

- Teste de integração (estilo do `OrderCreationIntegrationTest`): após processar um pedido,
  `/actuator/prometheus` contém `spring_kafka_listener_seconds_count` para os três listeners,
  e `orders_dead_lettered_total` existe.
- Validação manual documentada no README: `docker compose -f docker-compose.yml -f
  docker-compose.observability.yml up -d`, gerar carga com `POST /orders`, conferir dados
  no Zabbix (Monitoring → Latest data) e nos dois dashboards.
- README ganha seção "Observabilidade" com URLs, credenciais locais e comando de subida.

## Riscos

- **Templates YAML do Zabbix** são o artefato mais trabalhoso; o formato é validado ao
  importar, e o bootstrap falha alto se a importação for rejeitada.
- **Nomes de métricas** de `kafka.consumer.*` variam por versão; os itens são conferidos
  contra a saída real do endpoint durante a implementação.
- **`host.docker.internal`** em Linux depende de `host-gateway`; documentado no compose.
