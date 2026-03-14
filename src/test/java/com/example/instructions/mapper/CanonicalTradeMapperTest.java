package com.example.instructions.mapper;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.Trade;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CanonicalTradeMapperTest {

  private final CanonicalTradeMapper canonicalTradeMapper = Mappers.getMapper(CanonicalTradeMapper.class);

  @Test
  void shouldMaskAllButLastFourDigitsOfAccount() {
    final Trade trade = Trade.builder()
        .account("12345678")
        .security("abc123")
        .type("BUY")
        .amount(new BigDecimal("100.10"))
        .build();

    final CanonicalTrade canonicalTrade = canonicalTradeMapper.toCanonicalTrade(trade);

    assertEquals("****5678", canonicalTrade.account_number());
    assertEquals("ABC123", canonicalTrade.security_id());
    assertEquals("B", canonicalTrade.trade_type());
    assertEquals(new BigDecimal("100.10"), canonicalTrade.amount());
    assertNotNull(canonicalTrade.timestamp());
  }
}

