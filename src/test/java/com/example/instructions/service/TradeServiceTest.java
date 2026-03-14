package com.example.instructions.service;

import com.example.instructions.mapper.CanonicalTradeMapper;
import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.Trade;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TradeServiceTest {

  private final TradeService tradeService = new TradeService(Mappers.getMapper(CanonicalTradeMapper.class));

  @Test
  void shouldAcceptValidTradePatterns() {
    final List<CanonicalTrade> result = tradeService.processTrades(List.of(
            Trade.builder()
                .account("12345678")
                .security("ABC123")
                .type("buy")
                .amount(new BigDecimal("10.50"))
                .build(),
            Trade.builder()
                .account("87654321")
                .security("XYZ999")
                .type("S")
                .amount(new BigDecimal("20"))
                .build()))
        .collectList()
        .block();

    assertEquals(2, result == null ? 0 : result.size());
  }

  @Test
  void shouldRejectInvalidAccountPattern() {
    final List<CanonicalTrade> result = tradeService.processTrades(List.of(
            Trade.builder()
                .account("12A4567")
                .security("ABC123")
                .type("BUY")
                .amount(new BigDecimal("10"))
                .build()))
        .collectList()
        .block();

    assertEquals(0, result == null ? 0 : result.size());
  }

  @Test
  void shouldRejectInvalidSecurityPattern() {
    final List<CanonicalTrade> result = tradeService.processTrades(List.of(
            Trade.builder()
                .account("12345678")
                .security("AB1234")
                .type("SELL")
                .amount(new BigDecimal("10"))
                .build()))
        .collectList()
        .block();

    assertEquals(0, result == null ? 0 : result.size());
  }

  @Test
  void shouldRejectInvalidTypePattern() {
    final List<CanonicalTrade> result = tradeService.processTrades(List.of(
            Trade.builder()
                .account("12345678")
                .security("ABC123")
                .type("HOLD")
                .amount(new BigDecimal("10"))
                .build()))
        .collectList()
        .block();

    assertEquals(0, result == null ? 0 : result.size());
  }

}

