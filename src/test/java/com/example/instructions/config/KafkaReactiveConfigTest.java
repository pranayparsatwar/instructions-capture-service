package com.example.instructions.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.instructions.model.PlatformTrade;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;

@SpringBootTest(properties = "spring.kafka.admin.auto-create=false")
class KafkaReactiveConfigTest {

  @Autowired
  private KafkaSender<String, PlatformTrade> reactiveKafkaProducerTemplate;

  @Autowired
  private KafkaReceiver<String, PlatformTrade> reactiveKafkaConsumerTemplate;

  @Autowired
  private NewTopic outboundTopic;

  @Value("${app.kafka.outbound.topic:instructions.outbound}")
  private String configuredOutboundTopic;

  @Test
  void shouldCreateReactiveKafkaTemplates() {
    assertThat(reactiveKafkaProducerTemplate).isNotNull();
    assertThat(reactiveKafkaConsumerTemplate).isNotNull();
    assertThat(outboundTopic).isNotNull();
    assertThat(outboundTopic.name()).isEqualTo(configuredOutboundTopic);
  }
}

