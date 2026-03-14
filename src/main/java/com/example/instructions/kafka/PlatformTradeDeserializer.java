package com.example.instructions.kafka;

import com.example.instructions.model.PlatformTrade;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class PlatformTradeDeserializer implements Deserializer<PlatformTrade> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().findAndRegisterModules();

  @Override
  public PlatformTrade deserialize(final String topic, final byte[] data) {
    if (data == null || data.length == 0) {
      return null;
    }

    try {
      return OBJECT_MAPPER.readValue(data, PlatformTrade.class);
    } catch (Exception ex) {
      throw new SerializationException("Failed to deserialize PlatformTrade", ex);
    }
  }
}

