package com.example.instructions.model;

import java.math.BigDecimal;
import lombok.Builder;

@Builder(toBuilder = true)
public record Order(String account,
                    String security,
                    String type,
                    BigDecimal amount) {
  public static final byte VERSION = 1;
}
