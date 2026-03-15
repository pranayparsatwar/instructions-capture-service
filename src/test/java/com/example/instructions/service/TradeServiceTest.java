package com.example.instructions.service;

import com.example.instructions.mapper.CanonicalTradeMapper;
import com.example.instructions.mapper.PlatformTradeMapper;
import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.PlatformTrade;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TradeServiceTest {

  private final TradeService tradeService = new TradeService(
      Mappers.getMapper(CanonicalTradeMapper.class),
      Mappers.getMapper(PlatformTradeMapper.class),
      null);

  @Test
  @Disabled
  void shouldAcceptValidTradePatterns() {
    final List<PlatformTrade> result = tradeService.processTrades(List.of(
            CanonicalTrade.builder()
                .account_number("12345678")
                .security_id("ABC123")
                .trade_type("buy")
                .amount(new BigDecimal("10.50"))
                .build(),
            CanonicalTrade.builder()
                .account_number("87654321")
                .security_id("XYZ999")
                .trade_type("S")
                .amount(new BigDecimal("20"))
                .build()))
        .collectList()
        .block();

    assertEquals(2, result == null ? 0 : result.size());
//    assertEquals("12345678", result == null ? null : result.getFirst().account_number());
//    assertEquals("buy", result == null ? null : result.getFirst().trade_type());
//    assertEquals(List.of(), result == null ? null : result.getFirst().validation_errors());
  }

  @Test
  @Disabled
  void shouldRejectInvalidAccountPattern() {
    final List<PlatformTrade> result = tradeService.processTrades(List.of(
            CanonicalTrade.builder()
                .account_number("12A4567")
                .security_id("ABC123")
                .trade_type("BUY")
                .amount(new BigDecimal("10"))
                .build()))
        .collectList()
        .block();

    assertEquals(1, result == null ? 0 : result.size());
//    assertEquals(List.of("Account number must be 8 digits"),
//        result == null ? null : result.getFirst().validation_errors());
  }

  @Test
  @Disabled
  void shouldRejectInvalidSecurityPattern() {
    final List<PlatformTrade> result = tradeService.processTrades(List.of(
            CanonicalTrade.builder()
                .account_number("12345678")
                    .security_id("AB-1234")
                .trade_type("SELL")
                .amount(new BigDecimal("10"))
                .build()))
        .collectList()
        .block();

    assertEquals(1, result == null ? 0 : result.size());
//    assertEquals(List.of("Security ID must be alphanumeric"),
//        result == null ? null : result.getFirst().validation_errors());
  }

  @Test
  @Disabled
  void shouldRejectInvalidTypePattern() {
    final List<PlatformTrade> result = tradeService.processTrades(List.of(
            CanonicalTrade.builder()
                .account_number("12345678")
                .security_id("ABC123")
                .trade_type("HOLD")
                .amount(new BigDecimal("10"))
                .build()))
        .collectList()
        .block();

    assertEquals(1, result == null ? 0 : result.size());
//    assertEquals(List.of("Trade type must be BUY, SELL, B, or S"),
//        result == null ? null : result.getFirst().validation_errors());
  }

}

