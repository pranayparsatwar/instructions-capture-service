package com.example.instructions.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;

@SpringBootTest
class KafkaReactiveConfigTest {

  @Autowired
  private KafkaSender<String, String> reactiveKafkaProducerTemplate;

  @Autowired
  private KafkaReceiver<String, String> reactiveKafkaConsumerTemplate;

  @Test
  void shouldCreateReactiveKafkaTemplates() {
    assertThat(reactiveKafkaProducerTemplate).isNotNull();
    assertThat(reactiveKafkaConsumerTemplate).isNotNull();
  }
}

