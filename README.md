# order-events

[![CI](https://github.com/marcos-jesus/order-events/actions/workflows/ci.yml/badge.svg)](https://github.com/marcos-jesus/order-events/actions/workflows/ci.yml)

Exemplo de arquitetura orientada a eventos com **Spring Boot 3 (Java 21)** e **Apache Kafka**:
uma API REST publica um evento de domínio no Kafka, e três serviços independentes
(`inventory-service`, `notification-service` e `billing-service`) consomem esse mesmo
evento em paralelo, cada um em seu próprio consumer group — o padrão clássico de
desacoplamento via mensageria orientada a eventos.

```
POST /orders
      │
      ▼
OrderController ──► OrderEventPublisher ──► tópico "order-created" (Kafka)
                                                   │
                        ┌──────────────────────────┼──────────────────────────┐
                        ▼                           ▼                          ▼
        InventoryReservationListener   OrderNotificationListener   BillingObservableListener
           (group: inventory-service)   (group: notification-service)   (group: billing-service)
                                                                                  │
                                                                                  ▼
                                                                    Postgres (tabela billing_record)
```

## Por que três consumer groups?

No Kafka, consumidores de um mesmo **consumer group** dividem as partições entre si
(escalabilidade horizontal). Consumidores de **grupos diferentes** recebem, cada um,
uma cópia completa de todas as mensagens do tópico — é assim que múltiplos serviços
reagem de forma independente ao mesmo evento, sem acoplamento entre eles. O
`billing-service` é o único que persiste algo: a cada `OrderCreatedEvent`, grava um
`BillingRecord` no Postgres com o faturamento (`quantity × price`). Como a chave da
tabela é o próprio `orderId`, reprocessar o mesmo evento (entrega at-least-once do
Kafka) apenas regrava o mesmo registro — sem duplicar faturamento.

## Resiliência

Falhas de processamento no listener são reprocessadas 3x com backoff fixo
(`DefaultErrorHandler` + `FixedBackOff`); se ainda assim falharem, a mensagem é
publicada em uma dead-letter topic (`order-created.DLT`) via `DeadLetterPublishingRecoverer`,
em vez de travar a partição ou perder o evento.

## Rodando localmente

Sobe o Kafka (modo KRaft, sem Zookeeper), o [Kafka UI](https://github.com/provectus/kafka-ui)
para inspecionar tópicos e mensagens, e o Postgres onde o `billing-service` persiste o
faturamento:

```bash
docker compose up -d
```

Roda a aplicação (o Flyway cria o schema do Postgres automaticamente no boot):

```bash
./mvnw spring-boot:run
```

Cria um pedido:

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"product": "teclado-mecanico", "quantity": 2, "price": 150.00}'
```

Acompanhe os logs de `inventory-service`, `notification-service` e `billing-service`
reagindo ao mesmo evento, inspecione o tópico pelo Kafka UI em `http://localhost:8082`,
ou confira o faturamento persistido:

```bash
docker exec order-events-postgres psql -U order_events -d order_events \
  -c "SELECT * FROM billing_record;"
```

## Observabilidade (Zabbix + Grafana)

A app expõe métricas Micrometer em `http://localhost:8080/actuator/prometheus`. O Zabbix as
coleta (item HTTP agent + itens dependentes) e também monitora o Postgres via Agent2; o
Grafana exibe tudo pelo datasource do Zabbix. Métricas cobertas: requests HTTP (taxa,
latência, erros), itens processados e lag por consumer group, mensagens na DLT, pool de
conexões, JVM e banco (conexões, transações, tamanho, locks, linhas em `billing_record`).

Com o `docker compose up -d` e a app rodando (`./mvnw spring-boot:run`), suba a stack:

```bash
docker compose -f docker-compose.observability.yml up -d
```

O container `zabbix-bootstrap` importa os templates e cria os hosts sozinho (leva ~1 min).

| Serviço | URL | Login (somente ambiente local) |
|---|---|---|
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Zabbix | http://localhost:8083 | `Admin` / `zabbix` |

Dashboards no Grafana, pasta **Order Events**: *Aplicação* e *Banco*. Alertas (app fora do ar,
lag alto, 5xx, pool saturado, DLT, Postgres inacessível) aparecem em Monitoring → Problems
no Zabbix; os limites são macros do template (`{$LAG_MAX}`, `{$ERR_5XX_MAX}`, ...).

No Linux, se o Zabbix não alcançar a app, libere a porta 8080 para a rede do Docker no
firewall (o acesso é via `host.docker.internal`).

## Testes

```bash
./mvnw test
```

Os testes de integração sobem um broker Kafka embarcado (`@EmbeddedKafka`) e um Postgres
real via Testcontainers, sobem a aplicação inteira em uma porta aleatória, fazem a
chamada HTTP real e confirmam tanto a entrega do evento no tópico quanto a persistência
do faturamento na tabela `billing_record`.

## Stack

Java 21 · Spring Boot 3.3 · Spring Kafka · Spring Web · Bean Validation · Spring Data JPA
· Flyway · PostgreSQL · JUnit 5 · AssertJ · Testcontainers · Actuator · Micrometer · Zabbix · Grafana
