package com.example.instructions.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.PlatformTrade;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

class KafkaMessageServiceTest {

  @SuppressWarnings("unchecked")
  private static KafkaSender<String, PlatformTrade> mockKafkaSender() {
    return mock(KafkaSender.class);
  }

  @SuppressWarnings("unchecked")
  private static KafkaReceiver<String, PlatformTrade> mockKafkaReceiver() {
    return mock(KafkaReceiver.class);
  }

  @SuppressWarnings("unchecked")
  private static ReceiverRecord<String, PlatformTrade> mockReceiverRecord() {
    return mock(ReceiverRecord.class);
  }

  private static PlatformTrade sampleTrade(final String platformId) {
    return PlatformTrade.builder()
        .platform_id(platformId)
        .trade(CanonicalTrade.builder()
            .account_number("12345678")
            .security_id("ABC123")
            .trade_type("BUY")
            .amount(new BigDecimal("99.50"))
            .build())
        .build();
  }

  @Test
  void shouldPublishKafkaMessageToConfiguredTopic() {
    final KafkaSender<String, PlatformTrade> kafkaSender = mockKafkaSender();
    final KafkaReceiver<String, PlatformTrade> kafkaReceiver = mockKafkaReceiver();
    final KafkaMessageService kafkaMessageService =
        new KafkaMessageService(kafkaSender, kafkaReceiver, "instructions.outbound");
    final AtomicReference<SenderRecord<String, PlatformTrade, String>> capturedRecord = new AtomicReference<>();
    final PlatformTrade platformTrade = sampleTrade("trade-1");

    when(kafkaSender.send(any())).thenAnswer(invocation -> {
      final Publisher<SenderRecord<String, PlatformTrade, String>> publisher = invocation.getArgument(0);
      capturedRecord.set(Flux.from(publisher).blockFirst());
      return Flux.empty();
    });

    kafkaMessageService.send("trade-1", platformTrade).block();

    assertThat(capturedRecord.get()).isNotNull();
    assertThat(capturedRecord.get().topic()).isEqualTo("instructions.outbound");
    assertThat(capturedRecord.get().key()).isEqualTo("trade-1");
    assertThat(capturedRecord.get().value()).isEqualTo(platformTrade);
  }

  @Test
  void shouldAcknowledgeAndMapConsumedMessages() {
    final KafkaSender<String, PlatformTrade> kafkaSender = mockKafkaSender();
    final KafkaReceiver<String, PlatformTrade> kafkaReceiver = mockKafkaReceiver();
    final KafkaMessageService kafkaMessageService =
        new KafkaMessageService(kafkaSender, kafkaReceiver, "instructions.outbound");
    final ReceiverRecord<String, PlatformTrade> receiverRecord = mockReceiverRecord();
    final ReceiverOffset receiverOffset = mock(ReceiverOffset.class);
    final PlatformTrade platformTrade = sampleTrade("trade-2");

    when(receiverRecord.key()).thenReturn("trade-2");
    when(receiverRecord.value()).thenReturn(platformTrade);
    when(receiverRecord.topic()).thenReturn("instructions.outbound");
    when(receiverRecord.receiverOffset()).thenReturn(receiverOffset);
    when(kafkaReceiver.receive()).thenReturn(Flux.just(receiverRecord));

    final List<PlatformTrade> result = kafkaMessageService.receive().collectList().block();

    assertThat(result).containsExactly(platformTrade);
    verify(receiverOffset).acknowledge();
  }
}

