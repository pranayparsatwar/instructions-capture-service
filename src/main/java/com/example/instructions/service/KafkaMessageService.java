package com.example.instructions.service;

import com.example.instructions.model.PlatformTrade;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Service
@Slf4j
public class KafkaMessageService {

  private final KafkaSender<String, PlatformTrade> reactiveKafkaProducerTemplate;
  private final KafkaReceiver<String, PlatformTrade> reactiveKafkaConsumerTemplate;
  private final String topic;

  public KafkaMessageService(
      final KafkaSender<String, PlatformTrade> reactiveKafkaProducerTemplate,
      final KafkaReceiver<String, PlatformTrade> reactiveKafkaConsumerTemplate,
      final @Value("${app.kafka.outbound.topic:instructions.outbound}") String topic) {
    this.reactiveKafkaProducerTemplate = reactiveKafkaProducerTemplate;
    this.reactiveKafkaConsumerTemplate = reactiveKafkaConsumerTemplate;
    this.topic = topic;
  }

  public Mono<Void> send(final String key, final PlatformTrade platformTrade) {
    return sendPlatformTrade(key, platformTrade);
  }

  public Mono<Void> send(final PlatformTrade platformTrade) {
    return sendPlatformTrade(platformTrade.platform_id(), platformTrade);
  }

  private Mono<Void> sendPlatformTrade(final String key, final PlatformTrade platformTrade) {
    final SenderRecord<String, PlatformTrade, String> senderRecord = SenderRecord.create(topic, null,
        null, key, platformTrade, key);

    return reactiveKafkaProducerTemplate.send(Mono.just(senderRecord))
        .doOnNext(result -> log.info("Published PlatformTrade with key={} to topic={}", key, topic))
        .then();
  }

  public Flux<PlatformTrade> receive() {
    return reactiveKafkaConsumerTemplate.receive()
        .doOnNext(record -> {
          log.info("Received PlatformTrade with key={} from topic={}", record.key(), record.topic());
          record.receiverOffset().acknowledge();
        })
        .map(ConsumerRecord::value);
  }
}

