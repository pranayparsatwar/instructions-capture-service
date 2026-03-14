package com.example.instructions.config;

import java.util.List;
import java.util.Map;
import com.example.instructions.kafka.PlatformTradeDeserializer;
import com.example.instructions.kafka.PlatformTradeSerializer;
import com.example.instructions.model.PlatformTrade;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

@Configuration
public class KafkaReactiveConfig {

  private final KafkaProperties kafkaProperties;
  private final String topic;
  private final String groupId;
  private final int partitions;
  private final short replicationFactor;

  public KafkaReactiveConfig(
      final KafkaProperties kafkaProperties,
      final @Value("${app.kafka.outbound.topic:instructions.outbound}") String topic,
      final @Value("${app.kafka.outbound.group-id:instructions-capture-service}") String groupId,
      final @Value("${app.kafka.outbound.partitions:1}") int partitions,
      final @Value("${app.kafka.outbound.replication-factor:1}") short replicationFactor) {
    this.kafkaProperties = kafkaProperties;
    this.topic = topic;
    this.groupId = groupId;
    this.partitions = partitions;
    this.replicationFactor = replicationFactor;
  }

  @Bean
  public NewTopic outboundTopic() {
    return TopicBuilder.name(topic)
        .partitions(partitions)
        .replicas(replicationFactor)
        .build();
  }

  @Bean
  public KafkaSender<String, PlatformTrade> reactiveProducerTemplate() {
    final Map<String, Object> producerProperties = kafkaProperties.buildProducerProperties();
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PlatformTradeSerializer.class);

    final SenderOptions<String, PlatformTrade> senderOptions = SenderOptions.create(producerProperties);
    return KafkaSender.create(senderOptions);
  }

  @Bean
  public KafkaReceiver<String, PlatformTrade> reactiveConsumerTemplate() {
    final Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();
    consumerProperties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerProperties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PlatformTradeDeserializer.class);

    final ReceiverOptions<String, PlatformTrade> receiverOptions =
        ReceiverOptions.<String, PlatformTrade>create(consumerProperties)
            .subscription(List.of(topic));
    return KafkaReceiver.create(receiverOptions);
  }
}

