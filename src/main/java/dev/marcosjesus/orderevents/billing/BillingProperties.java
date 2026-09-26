package dev.marcosjesus.orderevents.billing;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Tamanho do lote do faturamento. Um único valor alinha os dois lados: é o max.poll.records
 * do consumer (quantos eventos chegam por poll) e o tamanho do batch gravado no Postgres,
 * então cada poll vira exatamente uma transação. O teto vem do limite de 65.535 parâmetros
 * por statement do Postgres: com 6 colunas por linha, o driver só consegue reescrever o lote
 * em um INSERT multi-linha até cerca de 10.900 linhas.
 */
@Validated
@ConfigurationProperties(prefix = "order-events.billing")
public record BillingProperties(
        @DefaultValue("5000") @Min(1) @Max(BillingProperties.MAX_BATCH_SIZE) int batchSize) {

    public static final int MAX_BATCH_SIZE = 10_000;
}
