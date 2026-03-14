package com.example.instructions.model;

import java.math.BigDecimal;
import java.time.Instant;
import lombok.Builder;

@Builder(toBuilder = true)
public record CanonicalTrade(String account_number,
                             String security_id,
                             String trade_type,
                             BigDecimal amount,
                             Instant timestamp) {
   public static final byte VERSION = 1;
}
