package com.example.instructions.service;

import com.example.instructions.model.CanonicalTrade;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TradeServiceTest {

  private final TradeService tradeService = new TradeService();

  @Test
  void shouldAcceptValidTradePatterns() {
    final List<CanonicalTrade> result = tradeService.processTrades(List.of(
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
    assertEquals("12345678", result == null ? null : result.getFirst().account_number());
    assertEquals("buy", result == null ? null : result.getFirst().trade_type());
  }

  @Test
  void shouldRejectInvalidAccountPattern() {
    final List<CanonicalTrade> result = tradeService.processTrades(List.of(
            CanonicalTrade.builder()
                .account_number("12A4567")
                .security_id("ABC123")
                .trade_type("BUY")
                .amount(new BigDecimal("10"))
                .build()))
        .collectList()
        .block();

    assertEquals(0, result == null ? 0 : result.size());
  }

  @Test
  void shouldRejectInvalidSecurityPattern() {
    final List<CanonicalTrade> result = tradeService.processTrades(List.of(
            CanonicalTrade.builder()
                .account_number("12345678")
                .security_id("AB1234")
                .trade_type("SELL")
                .amount(new BigDecimal("10"))
                .build()))
        .collectList()
        .block();

    assertEquals(0, result == null ? 0 : result.size());
  }

  @Test
  void shouldRejectInvalidTypePattern() {
    final List<CanonicalTrade> result = tradeService.processTrades(List.of(
            CanonicalTrade.builder()
                .account_number("12345678")
                .security_id("ABC123")
                .trade_type("HOLD")
                .amount(new BigDecimal("10"))
                .build()))
        .collectList()
        .block();

    assertEquals(0, result == null ? 0 : result.size());
  }

}

