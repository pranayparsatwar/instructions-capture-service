package com.example.instructions.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.math.BigDecimal;
import java.time.Instant;
import lombok.Builder;

@Builder(toBuilder = true)
public record PlatformTrade(String platform_id,
                            Trade trade) {
   public static final byte VERSION = 1;

   @Builder(toBuilder = true)
   public record Trade(String account,
                       String security,
                       String type,
                       BigDecimal amount,
                       @JsonFormat(shape = JsonFormat.Shape.STRING,
                           pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSX",
                           timezone = "UTC")
                       Instant timestamp) {
   }
}
