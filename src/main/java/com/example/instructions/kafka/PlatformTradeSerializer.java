package com.example.instructions.kafka;

import com.example.instructions.model.PlatformTrade;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class PlatformTradeSerializer implements Serializer<PlatformTrade> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().findAndRegisterModules();

  @Override
  public byte[] serialize(final String topic, final PlatformTrade data) {
    if (data == null) {
      return null;
    }

    try {
      return OBJECT_MAPPER.writeValueAsBytes(data);
    } catch (Exception ex) {
      throw new SerializationException("Failed to serialize PlatformTrade", ex);
    }
  }
}

