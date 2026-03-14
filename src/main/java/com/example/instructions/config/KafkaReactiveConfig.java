package com.example.instructions.config;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

@Configuration
public class KafkaReactiveConfig {

  private final KafkaProperties kafkaProperties;
  private final String topic;
  private final String groupId;

  public KafkaReactiveConfig(
      final KafkaProperties kafkaProperties,
      final @Value("${app.kafka.topic:trades.instructions}") String topic,
      final @Value("${app.kafka.group-id:instructions-capture-service}") String groupId) {
    this.kafkaProperties = kafkaProperties;
    this.topic = topic;
    this.groupId = groupId;
  }

  @Bean
  public KafkaSender<String, String> reactiveProducerTemplate() {
    final Map<String, Object> producerProperties = kafkaProperties.buildProducerProperties();
    producerProperties.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProperties.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    final SenderOptions<String, String> senderOptions = SenderOptions.create(producerProperties);
    return KafkaSender.create(senderOptions);
  }

  @Bean
  public KafkaReceiver<String, String> reactiveConsumerTemplate() {
    final Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();
    consumerProperties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerProperties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    final ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(consumerProperties)
        .subscription(List.of(topic));
    return KafkaReceiver.create(receiverOptions);
  }
}

