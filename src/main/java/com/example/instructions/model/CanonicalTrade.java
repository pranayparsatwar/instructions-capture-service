package com.example.instructions.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import java.math.BigDecimal;
import java.util.Comparator;
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

   public static final Comparator<CanonicalTrade> TIMESTAMP_DESC_COMPARATOR =
       Comparator.comparing(CanonicalTrade::timestamp, Comparator.nullsLast(Comparator.reverseOrder()));

   public enum TradeStatus {
     SUCCESS, FAILURE
   }
}
