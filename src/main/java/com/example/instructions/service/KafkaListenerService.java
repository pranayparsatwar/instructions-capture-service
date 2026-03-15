package com.example.instructions.service;

import com.example.instructions.model.CanonicalTrade;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.Disposable;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.util.retry.Retry;

//@Service
//@RequiredArgsConstructor
//@Slf4j
public class KafkaListenerService {

//  private final KafkaReceiver<String, CanonicalTrade> reactiveKafkaConsumerTemplate;
//  private final boolean listenerEnabled;
//
//  private Disposable listenerSubscription;
//
//  public KafkaListenerService(
//      final KafkaReceiver<String, CanonicalTrade> reactiveKafkaConsumerTemplate,
//      final @Value("${app.kafka.listener.enabled:true}") boolean listenerEnabled) {
//    this.reactiveKafkaConsumerTemplate = reactiveKafkaConsumerTemplate;
//    this.listenerEnabled = listenerEnabled;
//  }
//
//  public Flux<CanonicalTrade> receive() {
//    return reactiveKafkaConsumerTemplate.receive()
//        .doOnNext(record -> {
//          log.info("Received CanonicalTrade with key={} from topic={}", record.key(), record.topic());
//          record.receiverOffset().acknowledge();
//        })
//        .map(ConsumerRecord::value);
//  }
//
//  @PostConstruct
//  void startListener() {
//    if (!listenerEnabled) {
//      log.info("Kafka listener subscription is disabled by configuration");
//      return;
//    }
//
//    if (listenerSubscription != null && !listenerSubscription.isDisposed()) {
//      return;
//    }
//
//    listenerSubscription = receive()
//        .doOnSubscribe(subscription -> log.info("Starting Kafka listener subscription"))
//        .doOnError(error -> log.error("Kafka listener stream failed", error))
//        .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(5))
//            .maxBackoff(Duration.ofMinutes(1))
//            .doBeforeRetry(retrySignal -> log.warn(
//                "Retrying Kafka listener after failure, attempt={} cause={}",
//                retrySignal.totalRetries() + 1,
//                retrySignal.failure().getMessage())))
//        .subscribe(
//            trade -> log.debug("Consumed CanonicalTrade={}", trade),
//            error -> log.error("Kafka listener terminated unexpectedly", error));
//  }
//
//  @PreDestroy
//  void stopListener() {
//    if (listenerSubscription != null && !listenerSubscription.isDisposed()) {
//      listenerSubscription.dispose();
//      log.info("Stopped Kafka listener subscription");
//    }
//  }
}
