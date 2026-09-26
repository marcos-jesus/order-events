package dev.marcosjesus.orderevents.billing;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BillingPropertiesTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void aceitaOTamanhoDeLoteDe5MilEOLimiteDoPostgres() {
        assertThat(validator.validate(new BillingProperties(5000))).isEmpty();
        assertThat(validator.validate(new BillingProperties(BillingProperties.MAX_BATCH_SIZE))).isEmpty();
    }

    @Test
    void rejeitaLoteMaiorQueOLimiteDeParametrosDoPostgres() {
        assertThat(validator.validate(new BillingProperties(BillingProperties.MAX_BATCH_SIZE + 1))).hasSize(1);
    }

    @Test
    void rejeitaLoteZeroOuNegativo() {
        assertThat(validator.validate(new BillingProperties(0))).hasSize(1);
        assertThat(validator.validate(new BillingProperties(-1))).hasSize(1);
    }
}
