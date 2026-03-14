package com.example.instructions.mapper;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.Trade;
import java.util.Optional;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import org.mapstruct.NullValueCheckStrategy;

@Mapper(componentModel = "spring", nullValueCheckStrategy = NullValueCheckStrategy.ALWAYS)
public interface CanonicalTradeMapper {

  @Mapping(target = "account_number", source = "account", qualifiedByName = "toAccountNumber")
  @Mapping(target = "security_id", source = "security", qualifiedByName = "toSecurityId")
  @Mapping(target = "trade_type", source = "type", qualifiedByName = "toTradeType")
  @Mapping(target = "timestamp", expression = "java(java.time.Instant.now())")
  CanonicalTrade toCanonicalTrade(Trade trade);
  
  @Named("toAccountNumber")
  default String toAccountNumber(final String account) {
    return Optional.ofNullable(account)
        .map(String::trim)
        .filter(t -> !t.isEmpty())
        .map(value -> value.length() <= 4
            ? value
            : "*".repeat(value.length() - 4) + value.substring(value.length() - 4))
        .orElse(null);
  }

  @Named("toSecurityId")
  default String toSecurityId(final String security) {
    return Optional.ofNullable(security)
        .map(String::trim)
        .filter(t -> !t.isEmpty())
        .map(String::toUpperCase)
        .orElse(null);
  }

  @Named("toTradeType")
  default String toTradeType(final String type) {
    return Optional.ofNullable(type)
        .map(String::trim)
        .filter(t -> !t.isEmpty())
        .map(t -> String.valueOf(Character.toUpperCase(t.charAt(0))))
        .orElse(null);
  }
}
