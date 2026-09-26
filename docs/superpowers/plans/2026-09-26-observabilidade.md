# Observabilidade com Zabbix e Grafana — Plano de Implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dashboards no Grafana (fonte: Zabbix) com requests HTTP, itens processados/lag dos consumers Kafka e métricas do Postgres do `order-events`.

**Architecture:** A app expõe `/actuator/prometheus` (Micrometer). O Zabbix lê esse endpoint com um item HTTP agent mestre + itens dependentes (pré-processamento Prometheus, LLD), e lê o Postgres via Zabbix Agent2 (plugin PostgreSQL, uma query custom única). O Grafana usa o plugin de datasource do Zabbix, com datasource e dashboards provisionados por arquivo. Tudo sobe em `docker-compose.observability.yml`.

**Tech Stack:** Spring Boot 3.3.4 (Actuator, Micrometer Prometheus), Spring Kafka, Zabbix 7.0 (server, web, agent2), Grafana 11.2 + plugin `alexanderzobnin-zabbix-app`, Docker Compose, Postgres 16.

**Spec:** `docs/superpowers/specs/2026-09-26-observabilidade-design.md`

## Global Constraints

- `docker-compose.yml` e `docker-compose.cluster*.yml` NÃO são alterados; a stack nova é `docker-compose.observability.yml`.
- Sem Prometheus server. O Zabbix é a fonte única; Grafana só exibe.
- Kafka: só métricas expostas pela própria app (`kafka.consumer.*`); sem exporter/JMX do broker.
- `management.endpoints.web.exposure.include: health,info,prometheus` (nada além disso).
- Imagens com versão fixada no minor (`alpine-7.0-latest`, `grafana/grafana:11.2.0`, `postgres:16-alpine`); nunca `latest` puro.
- A app roda no host (porta 8080); containers a acessam por `host.docker.internal` (`extra_hosts: host.docker.internal:host-gateway`).
- Nomes dos listeners/consumer groups: `inventory-service`, `notification-service`, `billing-service`.
- Texto de comentários, logs e README em português (Brasil), com acentuação correta.
- **Working tree sujo:** `pom.xml`, `application.yml`, `README.md` e `BillingObservableListener.java` já têm alterações não commitadas do usuário. Nos commits, use `git add -p` e inclua SÓ os hunks deste plano; nunca `git add -A` nem `git commit -a`.

## Review Focus

- Métrica ainda inexistente (ex.: nenhum 5xx ainda, nenhum item na DLT): itens do Zabbix não podem virar "não suportado"; usar valor customizado `0` no erro do pré-processamento. `orders_dead_lettered_total` deve existir com valor 0 desde o boot (Task 2).
- App fora do ar: trigger de health dispara também por falta de dados (`nodata`), não só por `status != UP` (Task 4).
- Endpoints sensíveis do Actuator (`/actuator/env`) devem responder 404 (Task 1).
- Os três consumers devem aparecer com id estável no endpoint, senão o LLD do Zabbix perde a série (Task 1).
- Rodar o bootstrap duas vezes não pode duplicar hosts/usuários nem falhar (Task 4).

---

### Task 1: Actuator + Micrometer Prometheus e ids dos listeners

**Files:**
- Modify: `pom.xml` (dependências)
- Modify: `src/main/resources/application.yml`
- Modify: `src/main/java/dev/marcosjesus/orderevents/inventory/InventoryReservationListener.java`
- Modify: `src/main/java/dev/marcosjesus/orderevents/notification/OrderNotificationListener.java`
- Modify: `src/main/java/dev/marcosjesus/orderevents/billing/BillingObservableListener.java`
- Test: `src/test/java/dev/marcosjesus/orderevents/observability/ObservabilityIntegrationTest.java`

**Interfaces:**
- Produces: endpoint `GET /actuator/prometheus` com `http_server_requests_seconds_*`, `spring_kafka_listener_seconds_*` (um id por listener), `kafka_consumer_fetch_manager_records_lag_max`, `hikaricp_connections_*`, `jvm_*`. Consumido pelas Tasks 4 e 5.

- [ ] **Step 1: Escrever o teste que falha**

Criar `src/test/java/dev/marcosjesus/orderevents/observability/ObservabilityIntegrationTest.java`:

```java
package dev.marcosjesus.orderevents.observability;

import dev.marcosjesus.orderevents.kafka.KafkaTopics;
import dev.marcosjesus.orderevents.order.CreateOrderRequest;
import dev.marcosjesus.orderevents.order.OrderCreatedEvent;
import dev.marcosjesus.orderevents.support.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Garante o contrato que o Zabbix consome: o endpoint Prometheus expõe requests HTTP,
 * o processamento de cada consumer group e o lag do Kafka. @AutoConfigureObservability
 * é necessário porque, em @SpringBootTest, os exporters de métricas ficam desligados.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureObservability
@EmbeddedKafka(partitions = 1, topics = {KafkaTopics.ORDER_CREATED, KafkaTopics.ORDER_CREATED_DLT})
class ObservabilityIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void expoeMetricasDeRequestsConsumersELagNoEndpointPrometheus() {
        restTemplate.postForEntity("/orders",
                new CreateOrderRequest("teclado-mecanico", 1, new BigDecimal("10.00")), OrderCreatedEvent.class);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            String scrape = restTemplate.getForObject("/actuator/prometheus", String.class);

            assertThat(scrape).contains("http_server_requests_seconds_count");
            assertThat(scrape).contains("spring_kafka_listener_seconds_count");
            assertThat(scrape).contains("inventory-service", "notification-service", "billing-service");
            assertThat(scrape).contains("kafka_consumer_fetch_manager_records_lag_max");
            assertThat(scrape).contains("hikaricp_connections_active");
        });
    }

    @Test
    void naoExpoeEndpointsSensiveisDoActuator() {
        var response = restTemplate.getForEntity("/actuator/env", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `./mvnw -q test -Dtest=ObservabilityIntegrationTest`
Expected: FAIL (`/actuator/prometheus` retorna 404; ainda não há Actuator).

- [ ] **Step 3: Adicionar dependências**

Em `pom.xml`, dentro de `<dependencies>`, logo após o `spring-boot-starter-validation`:

```xml
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-registry-prometheus</artifactId>
        </dependency>
```

- [ ] **Step 4: Configurar `application.yml`**

Dentro de `spring.kafka:` adicionar (mesmo nível de `producer`/`consumer`):

```yaml
    listener:
      observation-enabled: true
```

E no fim do arquivo, no nível raiz:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus
  endpoint:
    health:
      probes:
        enabled: true
  metrics:
    tags:
      application: order-events
```

- [ ] **Step 5: Dar `id` explícito aos listeners**

Em cada listener, adicionar `id` igual ao consumer group:

`InventoryReservationListener.java`:
```java
    @KafkaListener(id = "inventory-service", topics = KafkaTopics.ORDER_CREATED, groupId = "inventory-service")
```
`OrderNotificationListener.java`:
```java
    @KafkaListener(id = "notification-service", topics = KafkaTopics.ORDER_CREATED, groupId = "notification-service")
```
`BillingObservableListener.java`:
```java
    @KafkaListener(id = "billing-service", topics = KafkaTopics.ORDER_CREATED, groupId = "billing-service")
```

- [ ] **Step 6: Rodar e ver passar; registrar nomes reais das métricas**

Run: `./mvnw -q test -Dtest=ObservabilityIntegrationTest`
Expected: PASS (2 testes).

Depois, subir a app (`docker compose up -d && ./mvnw spring-boot:run`), criar um pedido (comando do README) e anotar os nomes/labels reais que a Task 4 usa:

```bash
curl -s localhost:8080/actuator/prometheus | grep -E '^(spring_kafka_listener_seconds_count|kafka_consumer_fetch_manager_records_lag_max|http_server_requests_seconds_count|hikaricp_connections_(active|max))'
```
Expected: linhas com o label do id do listener (esperado `spring_kafka_listener_id="billing-service"`) e `client_id="consumer-billing-service-N"` no lag. **Se o nome do label do listener for diferente de `spring_kafka_listener_id`, use o nome real na Task 4** (uma ocorrência no LLD e as duas `parameters` dos protótipos de listener).

- [ ] **Step 7: Commit**

```bash
git add src/test/java/dev/marcosjesus/orderevents/observability/
git add -p pom.xml src/main/resources/application.yml src/main/java/dev/marcosjesus/orderevents/inventory src/main/java/dev/marcosjesus/orderevents/notification src/main/java/dev/marcosjesus/orderevents/billing/BillingObservableListener.java
git commit -m "feat: expõe métricas Prometheus (Actuator + Micrometer) e ids dos listeners"
```

---

### Task 2: Contador de mensagens enviadas para a DLT

**Files:**
- Create: `src/main/java/dev/marcosjesus/orderevents/kafka/CountingRecoverer.java`
- Modify: `src/main/java/dev/marcosjesus/orderevents/kafka/KafkaConsumerConfig.java`
- Test: `src/test/java/dev/marcosjesus/orderevents/kafka/CountingRecovererTest.java`
- Test: `src/test/java/dev/marcosjesus/orderevents/observability/ObservabilityIntegrationTest.java` (uma asserção)

**Interfaces:**
- Produces: `CountingRecoverer(ConsumerRecordRecoverer delegate, MeterRegistry registry)`; métrica `orders.dead_lettered` (Prometheus: `orders_dead_lettered_total`), registrada com valor 0 na construção. Consumida pela Task 4.

- [ ] **Step 1: Escrever o teste unitário que falha**

`src/test/java/dev/marcosjesus/orderevents/kafka/CountingRecovererTest.java`:

```java
package dev.marcosjesus.orderevents.kafka;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class CountingRecovererTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final ConsumerRecordRecoverer delegate = mock(ConsumerRecordRecoverer.class);
    private final ConsumerRecord<String, String> record = new ConsumerRecord<>("order-created", 0, 0L, "k", "v");

    @Test
    void existeComValorZeroAntesDaPrimeiraFalha() {
        new CountingRecoverer(delegate, registry);

        assertThat(registry.get("orders.dead_lettered").counter().count()).isZero();
    }

    @Test
    void contaCadaMensagemEnviadaParaADltEDelegaAoRecoverer() {
        var recoverer = new CountingRecoverer(delegate, registry);
        var erro = new RuntimeException("falha");

        recoverer.accept(record, erro);
        recoverer.accept(record, erro);

        assertThat(registry.get("orders.dead_lettered").counter().count()).isEqualTo(2.0);
        verify(delegate, org.mockito.Mockito.times(2)).accept(record, erro);
    }

    @Test
    void naoContaQuandoAPublicacaoNaDltFalha() {
        var recoverer = new CountingRecoverer(delegate, registry);
        doThrow(new IllegalStateException("broker fora")).when(delegate).accept(any(), any());

        assertThatThrownBy(() -> recoverer.accept(record, new RuntimeException("falha")))
                .isInstanceOf(IllegalStateException.class);

        assertThat(registry.get("orders.dead_lettered").counter().count()).isZero();
    }
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `./mvnw -q test -Dtest=CountingRecovererTest`
Expected: FAIL (compilação: `CountingRecoverer` não existe).

- [ ] **Step 3: Implementar**

`src/main/java/dev/marcosjesus/orderevents/kafka/CountingRecoverer.java`:

```java
package dev.marcosjesus.orderevents.kafka;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;

/**
 * Decora o recoverer da dead-letter topic contando cada mensagem que esgotou as tentativas.
 * Só conta depois que a publicação na DLT deu certo, para não contar duas vezes quando o
 * error handler repete a recuperação. O contador nasce com valor 0, assim a série existe
 * no endpoint Prometheus antes da primeira falha.
 */
public class CountingRecoverer implements ConsumerRecordRecoverer {

    private final ConsumerRecordRecoverer delegate;
    private final Counter deadLettered;

    public CountingRecoverer(ConsumerRecordRecoverer delegate, MeterRegistry registry) {
        this.delegate = delegate;
        this.deadLettered = Counter.builder("orders.dead_lettered")
                .description("Mensagens enviadas para a dead-letter topic após esgotar as tentativas")
                .register(registry);
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        delegate.accept(record, exception);
        deadLettered.increment();
    }
}
```

- [ ] **Step 4: Rodar e ver passar**

Run: `./mvnw -q test -Dtest=CountingRecovererTest`
Expected: PASS (3 testes).

- [ ] **Step 5: Ligar no `KafkaConsumerConfig`**

Em `KafkaConsumerConfig.java`: adicionar `import io.micrometer.core.instrument.MeterRegistry;` e trocar o bean por:

```java
    @Bean
    public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate, MeterRegistry meterRegistry) {
        var recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition())
        );
        return new DefaultErrorHandler(new CountingRecoverer(recoverer, meterRegistry), new FixedBackOff(1000L, 3L));
    }
```

- [ ] **Step 6: Asserção de integração**

Em `ObservabilityIntegrationTest`, dentro do `untilAsserted` do primeiro teste, adicionar:

```java
            assertThat(scrape).contains("orders_dead_lettered_total 0.0");
```

Run: `./mvnw -q test`
Expected: PASS (suíte inteira, incluindo `OrderCreationIntegrationTest`).

- [ ] **Step 7: Commit**

```bash
git add src/main/java/dev/marcosjesus/orderevents/kafka/ src/test/java/dev/marcosjesus/orderevents/kafka/ src/test/java/dev/marcosjesus/orderevents/observability/
git commit -m "feat: conta mensagens enviadas para a DLT (orders.dead_lettered)"
```

---

### Task 3: Compose de observabilidade (Zabbix, Agent2, Grafana) e usuário de monitoramento do Postgres

**Files:**
- Create: `docker-compose.observability.yml`
- Create: `observability/postgres/init-monitor-user.sql`
- Create: `observability/agent2/postgresql.conf`
- Create: `observability/agent2/custom-queries/stats.sql`

**Interfaces:**
- Produces: serviços `zabbix-db`, `zabbix-server`, `zabbix-web` (host `:8083`, login `Admin`/`zabbix`), `zabbix-agent2` (hostname `order-events-postgres`, sessão PostgreSQL `app`, query custom `stats`), `grafana` (`:3000`, `admin`/`admin`), `postgres-monitor-user` (one-shot), `zabbix-bootstrap` (one-shot, script criado na Task 4). Task 4 e 5 montam arquivos em `observability/zabbix` e `observability/grafana`.

- [ ] **Step 1: Script do usuário de monitoramento (idempotente)**

`observability/postgres/init-monitor-user.sql`:

```sql
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
```

- [ ] **Step 2: Configuração do plugin PostgreSQL do Agent2**

`observability/agent2/postgresql.conf`:

```
Plugins.PostgreSQL.Sessions.app.Uri=tcp://host.docker.internal:5432
Plugins.PostgreSQL.Sessions.app.User=zbx_monitor
Plugins.PostgreSQL.Sessions.app.Password=zbx_monitor
Plugins.PostgreSQL.Sessions.app.Database=order_events
Plugins.PostgreSQL.CustomQueriesPath=/etc/zabbix/custom-queries
```

- [ ] **Step 3: Query custom única**

`observability/agent2/custom-queries/stats.sql`:

```sql
SELECT
    (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database())          AS connections,
    (SELECT setting::int FROM pg_settings WHERE name = 'max_connections')               AS max_connections,
    (SELECT xact_commit FROM pg_stat_database WHERE datname = current_database())       AS xact_commit,
    (SELECT xact_rollback FROM pg_stat_database WHERE datname = current_database())     AS xact_rollback,
    pg_database_size(current_database())                                                AS db_size,
    (SELECT count(*) FROM pg_locks)                                                     AS locks,
    COALESCE(pg_total_relation_size(to_regclass('public.billing_record')), 0)           AS billing_size,
    COALESCE((SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = 'billing_record'), 0) AS billing_rows;
```

- [ ] **Step 4: Compose**

`docker-compose.observability.yml`:

```yaml
# Observabilidade: Zabbix (coleta/alertas) + Grafana (dashboards).
# Pré-requisito: `docker compose up -d` (Postgres da app em localhost:5432) e a app em localhost:8080.
# Subir: docker compose -f docker-compose.observability.yml up -d
name: order-events-observability

x-host-gateway: &host-gateway
  extra_hosts:
    - "host.docker.internal:host-gateway"

x-zabbix-db: &zabbix-db
  DB_SERVER_HOST: zabbix-db
  POSTGRES_USER: zabbix
  POSTGRES_PASSWORD: zabbix
  POSTGRES_DB: zabbix

services:
  zabbix-db:
    image: postgres:16-alpine
    container_name: order-events-zabbix-db
    environment:
      POSTGRES_USER: zabbix
      POSTGRES_PASSWORD: zabbix
      POSTGRES_DB: zabbix
    volumes:
      - zabbix-db-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U zabbix"]
      interval: 5s
      timeout: 3s
      retries: 20

  zabbix-server:
    image: zabbix/zabbix-server-pgsql:alpine-7.0-latest
    container_name: order-events-zabbix-server
    environment: *zabbix-db
    <<: *host-gateway
    depends_on:
      zabbix-db:
        condition: service_healthy

  zabbix-web:
    image: zabbix/zabbix-web-nginx-pgsql:alpine-7.0-latest
    container_name: order-events-zabbix-web
    ports:
      - "8083:8080"
    environment:
      <<: *zabbix-db
      ZBX_SERVER_HOST: zabbix-server
      PHP_TZ: America/Sao_Paulo
    depends_on:
      - zabbix-server

  zabbix-agent2:
    image: zabbix/zabbix-agent2:alpine-7.0-latest
    container_name: order-events-zabbix-agent2
    environment:
      ZBX_HOSTNAME: order-events-postgres
      ZBX_SERVER_HOST: zabbix-server
    <<: *host-gateway
    volumes:
      - ./observability/agent2/postgresql.conf:/etc/zabbix/zabbix_agent2.d/plugins.d/order-events-postgresql.conf:ro
      - ./observability/agent2/custom-queries:/etc/zabbix/custom-queries:ro
    depends_on:
      postgres-monitor-user:
        condition: service_completed_successfully

  postgres-monitor-user:
    image: postgres:16-alpine
    container_name: order-events-postgres-monitor-user
    restart: on-failure
    environment:
      PGPASSWORD: order_events
    <<: *host-gateway
    volumes:
      - ./observability/postgres/init-monitor-user.sql:/init.sql:ro
    command: ["psql", "-h", "host.docker.internal", "-U", "order_events", "-d", "order_events", "-v", "ON_ERROR_STOP=1", "-f", "/init.sql"]

  zabbix-bootstrap:
    image: alpine:3.20
    container_name: order-events-zabbix-bootstrap
    restart: on-failure
    environment:
      ZBX_URL: http://zabbix-web:8080/api_jsonrpc.php
    volumes:
      - ./observability/zabbix:/zabbix:ro
    command: ["sh", "-c", "apk add --no-cache curl jq >/dev/null && sh /zabbix/bootstrap.sh"]
    depends_on:
      - zabbix-web

  grafana:
    image: grafana/grafana:11.2.0
    container_name: order-events-grafana
    ports:
      - "3000:3000"
    environment:
      GF_INSTALL_PLUGINS: alexanderzobnin-zabbix-app
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - ./observability/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./observability/grafana/dashboards:/var/lib/grafana/dashboards:ro
      - grafana-data:/var/lib/grafana
    depends_on:
      - zabbix-web

volumes:
  zabbix-db-data:
  grafana-data:
```

Nota: `grafana-data` monta em `/var/lib/grafana` e `dashboards` em subpasta dele; o bind `dashboards` (ro) sobrepõe a subpasta sem conflito.

- [ ] **Step 5: Validar sintaxe e subir a parte de banco/agent**

```bash
docker compose -f docker-compose.observability.yml config -q && echo OK
docker compose up -d postgres
docker compose -f docker-compose.observability.yml up -d zabbix-db zabbix-server zabbix-web postgres-monitor-user zabbix-agent2
docker compose -f docker-compose.observability.yml logs postgres-monitor-user
```
Expected: `OK`; log do one-shot com `DO`, `GRANT ROLE`, `GRANT` e status `Exited (0)`.

- [ ] **Step 6: Validar o Agent2 contra o Postgres**

```bash
docker exec order-events-zabbix-agent2 zabbix_agent2 -t 'pgsql.ping[app]'
docker exec order-events-zabbix-agent2 zabbix_agent2 -t 'pgsql.query.custom[app,,,,stats]'
```
Expected: `pgsql.ping[app]  [s|1]` e um JSON como `[{"connections":N,"max_connections":100,...,"billing_rows":0}]`.

Se a chave custom for rejeitada (assinatura varia entre versões do plugin), teste as variantes `pgsql.query.custom[app,stats]` e `pgsql.query.custom[app,,,order_events,stats]`, escolha a que retorna o JSON e **use exatamente essa chave na Task 4** (item mestre do template Postgres). Se o plugin PostgreSQL não estiver na imagem (erro "unknown metric"), veja `docker exec order-events-zabbix-agent2 ls /usr/sbin/zabbix-agent2-plugin` e ajuste `Plugins.PostgreSQL.System.Path`.

- [ ] **Step 7: Commit**

```bash
git add docker-compose.observability.yml observability/postgres observability/agent2
git commit -m "feat: compose de observabilidade com Zabbix, Agent2 e Grafana"
```

---

### Task 4: Templates Zabbix e bootstrap

**Files:**
- Create: `observability/zabbix/templates/order-events-app.yaml`
- Create: `observability/zabbix/templates/order-events-postgres.yaml`
- Create: `observability/zabbix/bootstrap.sh`

**Interfaces:**
- Consumes: métricas da Task 1/2 (nomes/labels confirmados no Step 6 da Task 1); chave custom confirmada no Step 6 da Task 3.
- Produces: hosts `order-events-app` (template `Order Events App`) e `order-events-postgres` (template `Order Events Postgres`) no grupo `Order Events`; usuário Zabbix `grafana` / senha `Gr4f-ro-Zbx-2026` (somente leitura no grupo). Nomes de item exatos (usados pelos dashboards na Task 5):
  - App: `App: health`, `HTTP: requests/s`, `HTTP: 4xx/s`, `HTTP: 5xx/s`, `HTTP: max latency`, `HTTP: avg latency`, `JVM: heap used`, `JVM: threads live`, `Hikari: active connections`, `Hikari: max connections`, `DLT: messages total`, LLD `Listener {#LISTENER}: processed/s`, `Listener {#LISTENER}: failed/s`, `Consumer {#CLIENT}: lag max`.
  - Postgres: `PG: ping`, `PG: connections`, `PG: connections %`, `PG: commits/s`, `PG: rollbacks/s`, `PG: database size`, `PG: locks`, `PG: billing_record size`, `PG: billing_record rows`.

Os templates usam `@UUID@` como marcador de cada `uuid` (o Zabbix exige UUID v4 em cada entidade); o Step 3 substitui todos por UUIDs reais, uma única vez.

- [ ] **Step 1: Template da app**

`observability/zabbix/templates/order-events-app.yaml`:

```yaml
zabbix_export:
  version: '7.0'
  template_groups:
    - uuid: '@UUID@'
      name: 'Templates/Order Events'
  templates:
    - uuid: '@UUID@'
      template: 'Order Events App'
      name: 'Order Events App'
      description: 'Métricas do order-events lidas de /actuator/prometheus (Micrometer).'
      groups:
        - name: 'Templates/Order Events'
      macros:
        - macro: '{$APP_URL}'
          value: 'http://host.docker.internal:8080'
        - macro: '{$LAG_MAX}'
          value: '1000'
        - macro: '{$ERR_5XX_MAX}'
          value: '0.5'
        - macro: '{$HIKARI_USED_PCT}'
          value: '90'
      items:
        - uuid: '@UUID@'
          name: 'Prometheus raw'
          type: HTTP_AGENT
          key: order.prometheus.raw
          delay: 30s
          history: '0'
          trends: '0'
          value_type: TEXT
          url: '{$APP_URL}/actuator/prometheus'
          timeout: 10s
        - uuid: '@UUID@'
          name: 'App: health'
          type: HTTP_AGENT
          key: order.health
          delay: 30s
          history: 7d
          trends: '0'
          value_type: CHAR
          url: '{$APP_URL}/actuator/health'
          timeout: 10s
          status_codes: '200,503'
          preprocessing:
            - type: JSONPATH
              parameters:
                - $.status
          triggers:
            - uuid: '@UUID@'
              expression: 'last(/Order Events App/order.health)<>"UP" or nodata(/Order Events App/order.health,3m)=1'
              name: 'order-events: aplicação fora do ar'
              priority: DISASTER
        - uuid: '@UUID@'
          name: 'HTTP: requests/s'
          type: DEPENDENT
          key: order.http.requests.rate
          delay: '0'
          value_type: FLOAT
          units: rps
          preprocessing:
            - type: PROMETHEUS_PATTERN
              parameters:
                - 'http_server_requests_seconds_count{uri!~"/actuator.*"}'
                - sum
                - ''
              error_handler: CUSTOM_VALUE
              error_handler_params: '0'
            - type: CHANGE_PER_SECOND
              parameters:
                - ''
          master_item:
            key: order.prometheus.raw
        - uuid: '@UUID@'
          name: 'HTTP: 4xx/s'
          type: DEPENDENT
          key: order.http.4xx.rate
          delay: '0'
          value_type: FLOAT
          units: rps
          preprocessing:
            - type: PROMETHEUS_PATTERN
              parameters:
                - 'http_server_requests_seconds_count{outcome="CLIENT_ERROR",uri!~"/actuator.*"}'
                - sum
                - ''
              error_handler: CUSTOM_VALUE
              error_handler_params: '0'
            - type: CHANGE_PER_SECOND
              parameters:
                - ''
          master_item:
            key: order.prometheus.raw
        - uuid: '@UUID@'
          name: 'HTTP: 5xx/s'
          type: DEPENDENT
          key: order.http.5xx.rate
          delay: '0'
          value_type: FLOAT
          units: rps
          preprocessing:
            - type: PROMETHEUS_PATTERN
              parameters:
                - 'http_server_requests_seconds_count{outcome="SERVER_ERROR",uri!~"/actuator.*"}'
                - sum
                - ''
              error_handler: CUSTOM_VALUE
              error_handler_params: '0'
            - type: CHANGE_PER_SECOND
              parameters:
                - ''
          master_item:
            key: order.prometheus.raw
          triggers:
            - uuid: '@UUID@'
              expression: 'min(/Order Events App/order.http.5xx.rate,5m)>{$ERR_5XX_MAX}'
              name: 'order-events: taxa de erros 5xx alta'
              priority: HIGH
        - uuid: '@UUID@'
          name: 'HTTP: time sum/s'
          type: DEPENDENT
          key: order.http.time.rate
          delay: '0'
          value_type: FLOAT
          units: s
          description: 'Auxiliar da latência média (soma dos tempos por segundo).'
          preprocessing:
            - type: PROMETHEUS_PATTERN
              parameters:
                - 'http_server_requests_seconds_sum{uri!~"/actuator.*"}'
                - sum
                - ''
              error_handler: CUSTOM_VALUE
              error_handler_params: '0'
            - type: CHANGE_PER_SECOND
              parameters:
                - ''
          master_item:
            key: order.prometheus.raw
        - uuid: '@UUID@'
          name: 'HTTP: avg latency'
          type: CALCULATED
          key: order.http.latency.avg
          delay: 30s
          value_type: FLOAT
          units: s
          params: 'last(//order.http.time.rate) / max(last(//order.http.requests.rate), 0.000001)'
        - uuid: '@UUID@'
          name: 'HTTP: max latency'
          type: DEPENDENT
          key: order.http.latency.max
          delay: '0'
          value_type: FLOAT
          units: s
          preprocessing:
            - type: PROMETHEUS_PATTERN
              parameters:
                - 'http_server_requests_seconds_max{uri!~"/actuator.*"}'
                - max
                - ''
              error_handler: CUSTOM_VALUE
              error_handler_params: '0'
          master_item:
            key: order.prometheus.raw
        - uuid: '@UUID@'
          name: 'JVM: heap used'
          type: DEPENDENT
          key: order.jvm.heap.used
          delay: '0'
          value_type: FLOAT
          units: B
          preprocessing:
            - type: PROMETHEUS_PATTERN
              parameters:
                - 'jvm_memory_used_bytes{area="heap"}'
                - sum
                - ''
          master_item:
            key: order.prometheus.raw
        - uuid: '@UUID@'
          name: 'JVM: threads live'
          type: DEPENDENT
          key: order.jvm.threads.live
          delay: '0'
          value_type: FLOAT
          preprocessing:
            - type: PROMETHEUS_PATTERN
              parameters:
                - jvm_threads_live_threads
                - value
                - ''
          master_item:
            key: order.prometheus.raw
        - uuid: '@UUID@'
          name: 'Hikari: active connections'
          type: DEPENDENT
          key: order.hikari.active
          delay: '0'
          value_type: FLOAT
          preprocessing:
            - type: PROMETHEUS_PATTERN
              parameters:
                - hikaricp_connections_active
                - sum
                - ''
          master_item:
            key: order.prometheus.raw
          triggers:
            - uuid: '@UUID@'
              expression: 'min(/Order Events App/order.hikari.active,3m) >= last(/Order Events App/order.hikari.max) * {$HIKARI_USED_PCT} / 100'
              name: 'order-events: pool de conexões do banco saturado'
              priority: HIGH
        - uuid: '@UUID@'
          name: 'Hikari: max connections'
          type: DEPENDENT
          key: order.hikari.max
          delay: '0'
          value_type: FLOAT
          preprocessing:
            - type: PROMETHEUS_PATTERN
              parameters:
                - hikaricp_connections_max
                - sum
                - ''
          master_item:
            key: order.prometheus.raw
        - uuid: '@UUID@'
          name: 'DLT: messages total'
          type: DEPENDENT
          key: order.dlt.total
          delay: '0'
          value_type: FLOAT
          preprocessing:
            - type: PROMETHEUS_PATTERN
              parameters:
                - orders_dead_lettered_total
                - value
                - ''
              error_handler: CUSTOM_VALUE
              error_handler_params: '0'
          master_item:
            key: order.prometheus.raw
          triggers:
            - uuid: '@UUID@'
              expression: 'change(/Order Events App/order.dlt.total)>0'
              name: 'order-events: mensagem enviada para a dead-letter topic'
              priority: WARNING
      discovery_rules:
        - uuid: '@UUID@'
          name: 'Kafka listeners'
          type: DEPENDENT
          key: order.listeners.discovery
          delay: '0'
          master_item:
            key: order.prometheus.raw
          preprocessing:
            - type: PROMETHEUS_TO_JSON
              parameters:
                - spring_kafka_listener_seconds_count
          lld_macro_paths:
            - lld_macro: '{#LISTENER}'
              path: $.labels.spring_kafka_listener_id
          item_prototypes:
            - uuid: '@UUID@'
              name: 'Listener {#LISTENER}: processed/s'
              type: DEPENDENT
              key: 'order.listener.processed.rate[{#LISTENER}]'
              delay: '0'
              value_type: FLOAT
              units: eps
              preprocessing:
                - type: PROMETHEUS_PATTERN
                  parameters:
                    - 'spring_kafka_listener_seconds_count{spring_kafka_listener_id="{#LISTENER}",result="success"}'
                    - sum
                    - ''
                  error_handler: CUSTOM_VALUE
                  error_handler_params: '0'
                - type: CHANGE_PER_SECOND
                  parameters:
                    - ''
              master_item:
                key: order.prometheus.raw
            - uuid: '@UUID@'
              name: 'Listener {#LISTENER}: failed/s'
              type: DEPENDENT
              key: 'order.listener.failed.rate[{#LISTENER}]'
              delay: '0'
              value_type: FLOAT
              units: eps
              preprocessing:
                - type: PROMETHEUS_PATTERN
                  parameters:
                    - 'spring_kafka_listener_seconds_count{spring_kafka_listener_id="{#LISTENER}",result="failure"}'
                    - sum
                    - ''
                  error_handler: CUSTOM_VALUE
                  error_handler_params: '0'
                - type: CHANGE_PER_SECOND
                  parameters:
                    - ''
              master_item:
                key: order.prometheus.raw
        - uuid: '@UUID@'
          name: 'Kafka consumers'
          type: DEPENDENT
          key: order.consumers.discovery
          delay: '0'
          master_item:
            key: order.prometheus.raw
          preprocessing:
            - type: PROMETHEUS_TO_JSON
              parameters:
                - kafka_consumer_fetch_manager_records_lag_max
          lld_macro_paths:
            - lld_macro: '{#CLIENT}'
              path: $.labels.client_id
          item_prototypes:
            - uuid: '@UUID@'
              name: 'Consumer {#CLIENT}: lag max'
              type: DEPENDENT
              key: 'order.consumer.lag[{#CLIENT}]'
              delay: '0'
              value_type: FLOAT
              preprocessing:
                - type: PROMETHEUS_PATTERN
                  parameters:
                    - 'kafka_consumer_fetch_manager_records_lag_max{client_id="{#CLIENT}"}'
                    - max
                    - ''
                  error_handler: CUSTOM_VALUE
                  error_handler_params: '0'
              master_item:
                key: order.prometheus.raw
              trigger_prototypes:
                - uuid: '@UUID@'
                  expression: 'min(/Order Events App/order.consumer.lag[{#CLIENT}],5m)>{$LAG_MAX}'
                  name: 'order-events: lag alto no consumer {#CLIENT}'
                  priority: AVERAGE
```

- [ ] **Step 2: Template do Postgres**

`observability/zabbix/templates/order-events-postgres.yaml` (troque `pgsql.query.custom[{$PG.SESSION},,,,stats]` pela variante confirmada no Step 6 da Task 3, se diferente):

```yaml
zabbix_export:
  version: '7.0'
  template_groups:
    - uuid: '@UUID@'
      name: 'Templates/Order Events'
  templates:
    - uuid: '@UUID@'
      template: 'Order Events Postgres'
      name: 'Order Events Postgres'
      description: 'Postgres do order-events via Zabbix Agent2 (plugin PostgreSQL) e query custom stats.sql.'
      groups:
        - name: 'Templates/Order Events'
      macros:
        - macro: '{$PG.SESSION}'
          value: app
        - macro: '{$PG.CONN_PCT}'
          value: '80'
      items:
        - uuid: '@UUID@'
          name: 'PG: ping'
          key: 'pgsql.ping[{$PG.SESSION}]'
          delay: 30s
          history: 7d
          triggers:
            - uuid: '@UUID@'
              expression: 'last(/Order Events Postgres/pgsql.ping[{$PG.SESSION}])=0 or nodata(/Order Events Postgres/pgsql.ping[{$PG.SESSION}],3m)=1'
              name: 'order-events: Postgres inacessível'
              priority: DISASTER
        - uuid: '@UUID@'
          name: 'PG: stats raw'
          key: 'pgsql.query.custom[{$PG.SESSION},,,,stats]'
          delay: 30s
          history: '0'
          trends: '0'
          value_type: TEXT
        - uuid: '@UUID@'
          name: 'PG: connections'
          type: DEPENDENT
          key: pg.stats.connections
          delay: '0'
          preprocessing:
            - type: JSONPATH
              parameters:
                - '$[0].connections'
          master_item:
            key: 'pgsql.query.custom[{$PG.SESSION},,,,stats]'
        - uuid: '@UUID@'
          name: 'PG: max connections'
          type: DEPENDENT
          key: pg.stats.max_connections
          delay: '0'
          preprocessing:
            - type: JSONPATH
              parameters:
                - '$[0].max_connections'
          master_item:
            key: 'pgsql.query.custom[{$PG.SESSION},,,,stats]'
        - uuid: '@UUID@'
          name: 'PG: connections %'
          type: CALCULATED
          key: pg.stats.connections.pct
          delay: 30s
          value_type: FLOAT
          units: '%'
          params: 'last(//pg.stats.connections) / max(last(//pg.stats.max_connections), 1) * 100'
          triggers:
            - uuid: '@UUID@'
              expression: 'min(/Order Events Postgres/pg.stats.connections.pct,5m)>{$PG.CONN_PCT}'
              name: 'order-events: conexões do Postgres próximas do máximo'
              priority: HIGH
        - uuid: '@UUID@'
          name: 'PG: commits/s'
          type: DEPENDENT
          key: pg.stats.commits.rate
          delay: '0'
          value_type: FLOAT
          preprocessing:
            - type: JSONPATH
              parameters:
                - '$[0].xact_commit'
            - type: CHANGE_PER_SECOND
              parameters:
                - ''
          master_item:
            key: 'pgsql.query.custom[{$PG.SESSION},,,,stats]'
        - uuid: '@UUID@'
          name: 'PG: rollbacks/s'
          type: DEPENDENT
          key: pg.stats.rollbacks.rate
          delay: '0'
          value_type: FLOAT
          preprocessing:
            - type: JSONPATH
              parameters:
                - '$[0].xact_rollback'
            - type: CHANGE_PER_SECOND
              parameters:
                - ''
          master_item:
            key: 'pgsql.query.custom[{$PG.SESSION},,,,stats]'
        - uuid: '@UUID@'
          name: 'PG: database size'
          type: DEPENDENT
          key: pg.stats.db_size
          delay: '0'
          units: B
          preprocessing:
            - type: JSONPATH
              parameters:
                - '$[0].db_size'
          master_item:
            key: 'pgsql.query.custom[{$PG.SESSION},,,,stats]'
        - uuid: '@UUID@'
          name: 'PG: locks'
          type: DEPENDENT
          key: pg.stats.locks
          delay: '0'
          preprocessing:
            - type: JSONPATH
              parameters:
                - '$[0].locks'
          master_item:
            key: 'pgsql.query.custom[{$PG.SESSION},,,,stats]'
        - uuid: '@UUID@'
          name: 'PG: billing_record size'
          type: DEPENDENT
          key: pg.stats.billing_size
          delay: '0'
          units: B
          preprocessing:
            - type: JSONPATH
              parameters:
                - '$[0].billing_size'
          master_item:
            key: 'pgsql.query.custom[{$PG.SESSION},,,,stats]'
        - uuid: '@UUID@'
          name: 'PG: billing_record rows'
          type: DEPENDENT
          key: pg.stats.billing_rows
          delay: '0'
          preprocessing:
            - type: JSONPATH
              parameters:
                - '$[0].billing_rows'
          master_item:
            key: 'pgsql.query.custom[{$PG.SESSION},,,,stats]'
```

- [ ] **Step 3: Gerar os UUIDs (uma única vez) e conferir**

Os dois templates compartilham o grupo `Templates/Order Events`; o UUID do grupo deve ser IGUAL nos dois arquivos, então fixe-o antes de gerar os demais:

```bash
cd observability/zabbix/templates
GROUP_UUID=$(uuidgen | tr -d '-')
# 1ª ocorrência de @UUID@ em cada arquivo é o template_group
for f in order-events-app.yaml order-events-postgres.yaml; do
  perl -0pi -e "s/\@UUID\@/$GROUP_UUID/" "$f"
done
# demais UUIDs, um novo por ocorrência
perl -pi -e 'BEGIN{} s/\@UUID\@/do{my $u=`uuidgen`;chomp $u;$u=~s{-}{}g;$u}/ge' order-events-app.yaml order-events-postgres.yaml
grep -c '@UUID@' *.yaml
grep -h 'uuid:' *.yaml | sort | uniq -d
```
Expected: `0` para os dois arquivos; `uniq -d` imprime só a linha do UUID do grupo (repetida de propósito nos dois arquivos) e nenhuma outra.

- [ ] **Step 4: Script de bootstrap (idempotente)**

`observability/zabbix/bootstrap.sh`:

```sh
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
```

- [ ] **Step 5: Subir e validar o bootstrap (duas vezes)**

```bash
docker compose -f docker-compose.observability.yml up -d zabbix-bootstrap
docker compose -f docker-compose.observability.yml logs zabbix-bootstrap | tail -15
docker compose -f docker-compose.observability.yml run --rm zabbix-bootstrap | tail -6
```
Expected: 1ª execução termina com `bootstrap OK` (importados os 2 templates, hosts e usuário criados); 2ª execução também termina com `bootstrap OK`, imprimindo `host já existe` e sem erro.

Se `configuration.import` rejeitar o YAML, a mensagem da API aponta o caminho/campo inválido (o script falha alto); corrija o campo indicado no template e rode de novo.

- [ ] **Step 6: Validar que os dados chegam**

Suba a app (`./mvnw spring-boot:run`), gere carga e espere ~2 min:

```bash
for i in $(seq 1 20); do curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' -d '{"product":"teclado","quantity":2,"price":150.00}' >/dev/null; done
TOKEN=$(curl -s -H 'Content-Type: application/json-rpc' -d '{"jsonrpc":"2.0","method":"user.login","params":{"username":"Admin","password":"zabbix"},"id":1}' localhost:8083/api_jsonrpc.php | jq -r .result)
curl -s -H 'Content-Type: application/json-rpc' -H "Authorization: Bearer $TOKEN" \
  -d '{"jsonrpc":"2.0","method":"item.get","params":{"output":["name","lastvalue","state","error"],"host":"order-events-app","sortfield":"name"},"id":1}' \
  localhost:8083/api_jsonrpc.php | jq -r '.result[] | "\(.state)\t\(.lastvalue)\t\(.name)\t\(.error)"'
```
Expected: todos com `state 0` (suportado); `Listener ...: processed/s` para os 3 consumers e `Consumer ...: lag max` descobertos (o LLD leva até ~1 h no intervalo padrão; para acelerar, no Zabbix web, Data collection → Hosts → order-events-app → Discovery → "Execute now"). Repita a consulta com `"host":"order-events-postgres"` e confirme `PG: billing_record rows` > 0.

Item `state 1` (não suportado) → leia o `error`, corrija a chave/label no template e reimporte com `docker compose -f docker-compose.observability.yml run --rm zabbix-bootstrap`.

- [ ] **Step 7: Validar o trigger de app fora do ar**

Pare a app (Ctrl+C) e, após ~3 min:

```bash
curl -s -H 'Content-Type: application/json-rpc' -H "Authorization: Bearer $TOKEN" \
  -d '{"jsonrpc":"2.0","method":"problem.get","params":{"output":["name","severity"]},"id":1}' localhost:8083/api_jsonrpc.php | jq -r '.result[].name'
```
Expected: inclui `order-events: aplicação fora do ar`. Suba a app de novo e confirme que o problema é resolvido.

- [ ] **Step 8: Commit**

```bash
git add observability/zabbix
git commit -m "feat: templates Zabbix (app e Postgres) e bootstrap idempotente"
```

---

### Task 5: Grafana provisionado (datasource e dashboards)

**Files:**
- Create: `observability/grafana/provisioning/plugins/zabbix.yaml`
- Create: `observability/grafana/provisioning/datasources/zabbix.yaml`
- Create: `observability/grafana/provisioning/dashboards/dashboards.yaml`
- Create: `observability/grafana/gen_dashboards.py`
- Create (gerados): `observability/grafana/dashboards/aplicacao.json`, `observability/grafana/dashboards/banco.json`

**Interfaces:**
- Consumes: nomes de item exatos listados na Task 4; usuário `grafana` / `Gr4f-ro-Zbx-2026`; grupo `Order Events`; hosts `order-events-app`, `order-events-postgres`.
- Produces: datasource `Zabbix` (uid `zabbix`), pasta `Order Events` com os dashboards `order-events-app` e `order-events-db`.

- [ ] **Step 1: Provisionamento do plugin e do datasource**

`observability/grafana/provisioning/plugins/zabbix.yaml`:
```yaml
apiVersion: 1
apps:
  - type: alexanderzobnin-zabbix-app
    disabled: false
```

`observability/grafana/provisioning/datasources/zabbix.yaml`:
```yaml
apiVersion: 1
datasources:
  - name: Zabbix
    uid: zabbix
    type: alexanderzobnin-zabbix-datasource
    access: proxy
    url: http://zabbix-web:8080/api_jsonrpc.php
    isDefault: true
    jsonData:
      username: grafana
      trends: true
      trendsFrom: 7d
      trendsRange: 4d
      cacheTTL: 1m
    secureJsonData:
      password: Gr4f-ro-Zbx-2026
```

`observability/grafana/provisioning/dashboards/dashboards.yaml`:
```yaml
apiVersion: 1
providers:
  - name: order-events
    folder: Order Events
    type: file
    allowUiUpdates: false
    options:
      path: /var/lib/grafana/dashboards
```

- [ ] **Step 2: Gerador dos dashboards**

`observability/grafana/gen_dashboards.py` (gera os JSONs; rode com `python3` sempre que mudar um painel):

```python
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

app = dashboard("order-events-app", "Order Events — Aplicação", [
    panel(1, "Saúde da aplicação", APP, ["App: health"], 0, 0, w=6, h=4, ptype="stat"),
    panel(2, "Mensagens na DLT (total)", APP, ["DLT: messages total"], 6, 0, w=6, h=4, ptype="stat"),
    panel(3, "Requests/s", APP, ["HTTP: requests/s"], 0, 4, unit="reqps"),
    panel(4, "Erros HTTP/s (4xx e 5xx)", APP, ["HTTP: 4xx/s", "HTTP: 5xx/s"], 12, 4, unit="reqps"),
    panel(5, "Latência HTTP (média e máxima)", APP, ["HTTP: avg latency", "HTTP: max latency"], 0, 12, unit="s"),
    panel(6, "Itens processados/s por consumer", APP, ["/Listener .*: processed\\/s/"], 12, 12, unit="ops"),
    panel(7, "Falhas/s por consumer", APP, ["/Listener .*: failed\\/s/"], 0, 20, unit="ops"),
    panel(8, "Lag por consumer", APP, ["/Consumer .*: lag max/"], 12, 20),
    panel(9, "Heap JVM e threads", APP, ["JVM: heap used"], 0, 28, unit="bytes"),
    panel(10, "Pool Hikari (ativas vs máximo)", APP,
          ["Hikari: active connections", "Hikari: max connections"], 12, 28),
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
```

- [ ] **Step 3: Gerar e validar o JSON**

```bash
python3 observability/grafana/gen_dashboards.py
python3 -c "import json;[json.load(open(f'observability/grafana/dashboards/{n}.json')) for n in ('aplicacao','banco')];print('JSON OK')"
```
Expected: `gerado ...` duas vezes e `JSON OK`.

- [ ] **Step 4: Subir o Grafana e validar**

```bash
docker compose -f docker-compose.observability.yml up -d grafana
sleep 30
curl -s -u admin:admin localhost:3000/api/health | jq -r .database
curl -s -u admin:admin localhost:3000/api/datasources | jq -r '.[].name'
curl -s -u admin:admin 'localhost:3000/api/search?tag=order-events' | jq -r '.[].title'
curl -s -u admin:admin -X POST localhost:3000/api/datasources/uid/zabbix/health | jq -r .message
```
Expected: `ok`; `Zabbix`; os dois títulos dos dashboards; mensagem de sucesso na conexão com o Zabbix (`Zabbix API version: 7.0...`).

Depois, abra `http://localhost:3000` (admin/admin), pasta **Order Events**, com a app e a carga da Task 4 rodando, e confirme que os painéis mostram dados (sem "No data"). Painel vazio: confira se o texto do filtro do item é idêntico ao nome do item no Zabbix e ajuste em `gen_dashboards.py` (depois regenere).

- [ ] **Step 5: Commit**

```bash
git add observability/grafana
git commit -m "feat: Grafana provisionado com datasource Zabbix e dashboards de app e banco"
```

---

### Task 6: README e validação ponta a ponta

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Documentar no README**

Em `README.md`, antes da seção `## Testes`, inserir (usando `git add -p` depois, por causa das alterações prévias no arquivo):

````markdown
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
````

Na seção `## Stack`, acrescentar ao fim da lista: `· Actuator · Micrometer · Zabbix · Grafana`.

- [ ] **Step 2: Validação ponta a ponta do zero**

```bash
docker compose -f docker-compose.observability.yml down -v
docker compose -f docker-compose.observability.yml up -d
sleep 90
docker compose -f docker-compose.observability.yml ps -a --format '{{.Service}}\t{{.State}}\t{{.ExitCode}}'
```
Expected: `zabbix-db`, `zabbix-server`, `zabbix-web`, `zabbix-agent2`, `grafana` = `running`; `postgres-monitor-user` e `zabbix-bootstrap` = `exited` com código `0`.

Repita as checagens do Step 6 da Task 4 e do Step 4 da Task 5, gere carga e confirme nos dois dashboards: requests/s > 0, processed/s dos 3 consumers, lag ≈ 0 e linhas de `billing_record` crescendo.

- [ ] **Step 3: Suíte completa**

Run: `./mvnw -q test`
Expected: PASS (todos os testes, incluindo `ObservabilityIntegrationTest` e `CountingRecovererTest`).

- [ ] **Step 4: Commit**

```bash
git add -p README.md
git commit -m "docs: seção de observabilidade com Zabbix e Grafana"
```
