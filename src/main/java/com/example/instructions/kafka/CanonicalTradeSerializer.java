package com.example.instructions.kafka;

import com.example.instructions.model.CanonicalTrade;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class CanonicalTradeSerializer implements Serializer<CanonicalTrade> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().findAndRegisterModules();

  @Override
  public byte[] serialize(final String topic, final CanonicalTrade data) {
    if (data == null) {
      return null;
    }

    try {
      return OBJECT_MAPPER.writeValueAsBytes(data);
    } catch (Exception ex) {
      throw new SerializationException("Failed to serialize CanonicalTrade", ex);
    }
  }
}

