package dev.marcosjesus.orderevents.support;

import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base para testes que sobem o contexto Spring inteiro: como o BillingObservableListener
 * depende de um DataSource real (JPA + Flyway), qualquer teste que carregue a aplicação
 * inteira precisa de um Postgres — aqui via Testcontainers, com @ServiceConnection
 * configurando o datasource automaticamente.
 */
@Testcontainers
public abstract class AbstractPostgresIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
}
