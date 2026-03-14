package com.example.instructions.mapper;

import static com.example.instructions.util.TradeTransformer.ACCOUNT_PATTERN;
import static com.example.instructions.util.TradeTransformer.SECURITY_PATTERN;
import static com.example.instructions.util.TradeTransformer.TYPE_PATTERN;

import com.example.instructions.model.CanonicalTrade;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import org.mapstruct.NullValueCheckStrategy;

@Mapper(componentModel = "spring", nullValueCheckStrategy = NullValueCheckStrategy.ALWAYS)
public interface CanonicalTradeMapper {

  @Mapping(target = "status", ignore = true)
  @Mapping(target = "validation_errors", source = "canonicalTrade", qualifiedByName = "toValidationErrors")
  @Mapping(target = "timestamp", expression = "java(java.time.Instant.now())")
  CanonicalTrade toCanonicalTrade(CanonicalTrade canonicalTrade);

  @Named("toValidationErrors")
  default List<String> toValidationErrors(final CanonicalTrade canonicalTrade) {
    return Optional.ofNullable(canonicalTrade)
        .map(trade -> {
          List<String> errors = new ArrayList<>();
          if (trade.amount() == null) {
            errors.add("Amount is required");
          }
          if (trade.account_number() == null || !ACCOUNT_PATTERN.matcher(trade.account_number()).matches()) {
            errors.add("Account number must be 8 digits");
          }
          if (trade.security_id() == null || !SECURITY_PATTERN.matcher(trade.security_id()).matches()) {
            errors.add("Security ID must be alphanumeric");
          }
          if (trade.trade_type() == null || !TYPE_PATTERN.matcher(trade.trade_type()).matches()) {
            errors.add("Trade type must be BUY, SELL, B, or S");
          }
          return errors;
        })
        .orElse(List.of("Trade data is missing"));
  }
}
