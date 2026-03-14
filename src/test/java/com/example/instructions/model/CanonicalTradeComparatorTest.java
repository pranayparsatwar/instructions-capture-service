package com.example.instructions.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CanonicalTradeComparatorTest {

  @Test
  void shouldSortByTimestampDescendingWithNullsLast() {
    final CanonicalTrade older = CanonicalTrade.builder()
        .account_number("12345678")
        .security_id("ABC123")
        .trade_type("BUY")
        .timestamp(Instant.parse("2026-03-14T10:00:00Z"))
        .build();

    final CanonicalTrade newer = CanonicalTrade.builder()
        .account_number("87654321")
        .security_id("XYZ999")
        .trade_type("SELL")
        .timestamp(Instant.parse("2026-03-14T12:00:00Z"))
        .build();

    final CanonicalTrade noTimestamp = CanonicalTrade.builder()
        .account_number("11112222")
        .security_id("LMN456")
        .trade_type("B")
        .timestamp(null)
        .build();

    final List<CanonicalTrade> trades = new ArrayList<>(List.of(older, noTimestamp, newer));
    trades.sort(CanonicalTrade.TIMESTAMP_DESC_COMPARATOR);

    assertEquals(newer, trades.get(0));
    assertEquals(older, trades.get(1));
    assertEquals(noTimestamp, trades.get(2));
  }
}

