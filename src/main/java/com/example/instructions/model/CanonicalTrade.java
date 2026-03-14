package com.example.instructions.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import lombok.Builder;
import lombok.With;

@Builder(toBuilder = true)
public record CanonicalTrade(@JsonAlias("account")
                             String account_number,
                             @JsonAlias("security")
                             String security_id,
                             @JsonAlias("type")
                             String trade_type,
                             BigDecimal amount,
                             @With
                             TradeStatus status,
                             List<String> validation_errors,
                             Instant timestamp) {
   public static final byte VERSION = 1;

   public enum TradeStatus {
     SUCCESS, FAILURE
   }
}
