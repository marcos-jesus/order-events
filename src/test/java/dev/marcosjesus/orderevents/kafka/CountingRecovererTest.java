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
import static org.mockito.Mockito.times;
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
        verify(delegate, times(2)).accept(record, erro);
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
