package com.example.instructions.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import java.math.BigDecimal;
import java.time.Instant;
import lombok.Builder;

@Builder(toBuilder = true)
public record CanonicalTrade(@JsonAlias("account") String account_number,
                             @JsonAlias("security") String security_id,
                             @JsonAlias("type") String trade_type,
                             BigDecimal amount,
                             Instant timestamp) {
   public static final byte VERSION = 1;
}
