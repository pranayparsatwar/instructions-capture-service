package com.example.instructions.service;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.PlatformTrade;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaPublisher {

  private final KafkaSender<String, PlatformTrade> reactivePlatformTradeProducerTemplate;
  private final KafkaSender<String, CanonicalTrade> reactiveCanonicalTradeProducerTemplate;

  @Value("${app.kafka.outbound.topic:instructions.outbound}")
  private String outboundTopic;

  @Value("${app.kafka.inbound.topic:instructions.inbound}")
  private String inboundTopic;

  public Mono<Void> sendPlatformTrade(final PlatformTrade platformTrade) {
    final var key = platformTrade.platform_id();
    final var topic = outboundTopic;
    final SenderRecord<String, PlatformTrade, String> senderRecord = SenderRecord.create(
        topic, null, null, key, platformTrade, key);

    return reactivePlatformTradeProducerTemplate.send(Mono.just(senderRecord))
        .doOnNext(result -> log.info("Published PlatformTrade with key={} to topic={}", key, topic))
        .then();
  }

  public Mono<Void> sendCanonicalTrade(final CanonicalTrade canonicalTrade) {
    final var key = canonicalTrade.security_id();
    final var topic = inboundTopic;
    final SenderRecord<String, CanonicalTrade, String> senderRecord = SenderRecord.create(
        topic, null, null, key, canonicalTrade, key);

    return reactiveCanonicalTradeProducerTemplate.send(Mono.just(senderRecord))
        .doOnNext(result -> log.info("Published CanonicalTrade with key={} to topic={}", key, topic))
        .then();
  }
}
