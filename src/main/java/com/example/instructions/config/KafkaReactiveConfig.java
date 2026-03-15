package com.example.instructions.config;

import com.example.instructions.kafka.CanonicalTradeDeserializer;
import com.example.instructions.kafka.CanonicalTradeSerializer;
import com.example.instructions.kafka.PlatformTradeDeserializer;
import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.kafka.PlatformTradeSerializer;
import com.example.instructions.model.PlatformTrade;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
public class KafkaReactiveConfig {

  private final KafkaProperties kafkaProperties;

  @Value("${app.kafka.outbound.topic:instructions.outbound}")
  private String outboundTopic;
  @Value("${app.kafka.outbound.group-id:instructions-capture-service}")
  private String outboundGroupId;
  @Value("${app.kafka.outbound.partitions:1}")
  private int outboundPartitions;
  @Value("${app.kafka.outbound.replication-factor:1}")
  private short outboundReplicationFactor;

  @Value("${app.kafka.inbound.topic:instructions.inbound}")
  private String inboundTopic;
  @Value("${app.kafka.inbound.group-id:instructions-capture-service}")
  private String inboundGroupId;
  @Value("${app.kafka.inbound.partitions:1}")
  private int inboundPartitions;
  @Value("${app.kafka.inbound.replication-factor:1}")
  private short inboundReplicationFactor;

  @Bean
  public NewTopic outboundTopic() {
    return TopicBuilder.name(outboundTopic)
        .partitions(outboundPartitions)
        .replicas(outboundReplicationFactor)
        .build();
  }

  @Bean
  public NewTopic inboundTopic() {
    return TopicBuilder.name(inboundTopic)
        .partitions(inboundPartitions)
        .replicas(inboundReplicationFactor)
        .build();
  }

  @Bean
  public KafkaSender<String, PlatformTrade> reactivePlatformTradeProducerTemplate() {
    final Map<String, Object> producerProperties = kafkaProperties.buildProducerProperties();
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PlatformTradeSerializer.class);

    final SenderOptions<String, PlatformTrade> senderOptions = SenderOptions.create(producerProperties);
    return KafkaSender.create(senderOptions);
  }

  @Bean
  public KafkaReceiver<String, PlatformTrade> reactivePlatformTradeConsumerTemplate() {
    final Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();
    consumerProperties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, outboundGroupId);
    consumerProperties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PlatformTradeDeserializer.class);

    final ReceiverOptions<String, PlatformTrade> receiverOptions =
        ReceiverOptions.<String, PlatformTrade>create(consumerProperties)
            .subscription(List.of(outboundTopic));
    return KafkaReceiver.create(receiverOptions);
  }

  @Bean
  public KafkaSender<String, CanonicalTrade> reactiveCanonicalTradeProducerTemplate() {
    final Map<String, Object> producerProperties = kafkaProperties.buildProducerProperties();
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CanonicalTradeSerializer.class);

    final SenderOptions<String, CanonicalTrade> senderOptions = SenderOptions.create(producerProperties);
    return KafkaSender.create(senderOptions);
  }

  @Bean
  public KafkaReceiver<String, CanonicalTrade> reactiveCanonicalTradeConsumerTemplate() {
    final Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();
    consumerProperties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, inboundGroupId);
    consumerProperties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CanonicalTradeDeserializer.class);

    final ReceiverOptions<String, CanonicalTrade> receiverOptions =
        ReceiverOptions.<String, CanonicalTrade>create(consumerProperties)
            .subscription(List.of(inboundTopic));
    return KafkaReceiver.create(receiverOptions);
  }
}

