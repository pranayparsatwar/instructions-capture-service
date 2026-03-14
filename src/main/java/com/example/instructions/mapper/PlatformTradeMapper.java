package com.example.instructions.mapper;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.PlatformTrade;
import java.util.Optional;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import org.mapstruct.NullValueCheckStrategy;

@Mapper(componentModel = "spring", nullValueCheckStrategy = NullValueCheckStrategy.ALWAYS)
public interface PlatformTradeMapper {

  @Mapping(target = "platform_id", expression = "java(java.util.UUID.randomUUID().toString())")
  @Mapping(target = "trade.account_number", source = "account_number", qualifiedByName = "toAccountNumber")
  @Mapping(target = "trade.security_id", source = "security_id", qualifiedByName = "toSecurityId")
  @Mapping(target = "trade.trade_type", source = "trade_type", qualifiedByName = "toTradeType")
  @Mapping(target = "trade.amount", source = "amount")
  @Mapping(target = "trade.timestamp", expression = "java(java.time.Instant.now())")
  PlatformTrade toPlatformTrade(CanonicalTrade canonicalTrade);

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
