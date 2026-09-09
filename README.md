# order-events

[![CI](https://github.com/marcos-jesus/order-events/actions/workflows/ci.yml/badge.svg)](https://github.com/marcos-jesus/order-events/actions/workflows/ci.yml)

Exemplo de arquitetura orientada a eventos com **Spring Boot 3 (Java 21)** e **Apache Kafka**:
uma API REST publica um evento de domínio no Kafka, e dois serviços independentes
(`inventory-service` e `notification-service`) consomem esse mesmo evento em paralelo,
cada um em seu próprio consumer group — o padrão clássico de desacoplamento via
mensageria orientada a eventos.

```
POST /orders
      │
      ▼
OrderController ──► OrderEventPublisher ──► tópico "order-created" (Kafka)
                                                   │
                              ┌────────────────────┴────────────────────┐
                              ▼                                         ▼
                  InventoryReservationListener              OrderNotificationListener
                     (group: inventory-service)               (group: notification-service)
```

## Por que dois consumer groups?

No Kafka, consumidores de um mesmo **consumer group** dividem as partições entre si
(escalabilidade horizontal). Consumidores de **grupos diferentes** recebem, cada um,
uma cópia completa de todas as mensagens do tópico — é assim que múltiplos serviços
reagem de forma independente ao mesmo evento, sem acoplamento entre eles.

## Resiliência

Falhas de processamento no listener são reprocessadas 3x com backoff fixo
(`DefaultErrorHandler` + `FixedBackOff`); se ainda assim falharem, a mensagem é
publicada em uma dead-letter topic (`order-created.DLT`) via `DeadLetterPublishingRecoverer`,
em vez de travar a partição ou perder o evento.

## Rodando localmente

Sobe o Kafka (modo KRaft, sem Zookeeper) e o [Kafka UI](https://github.com/provectus/kafka-ui)
para inspecionar tópicos e mensagens:

```bash
docker compose up -d
```

Roda a aplicação:

```bash
./mvnw spring-boot:run
```

Cria um pedido:

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"product": "teclado-mecanico", "quantity": 2}'
```

Acompanhe os logs de `inventory-service` e `notification-service` reagindo ao mesmo
evento, ou inspecione o tópico pelo Kafka UI em `http://localhost:8081`.

## Testes

```bash
./mvnw test
```

O teste de integração sobe um broker Kafka embarcado (`@EmbeddedKafka`), sobe a
aplicação inteira em uma porta aleatória, faz a chamada HTTP real e confirma que o
evento publicado pelo controller chega de volta via um terceiro consumer group
dedicado ao teste.

## Stack

Java 21 · Spring Boot 3.3 · Spring Kafka · Spring Web · Bean Validation · JUnit 5 · AssertJ
