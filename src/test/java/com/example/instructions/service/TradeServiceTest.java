package com.example.instructions.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.instructions.mapper.CanonicalTradeMapper;
import com.example.instructions.mapper.PlatformTradeMapper;
import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.PlatformTrade;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mapstruct.factory.Mappers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class TradeServiceTest {

  @Mock
  private KafkaPublisher kafkaPublisher;

  private TradeService tradeService;

  @BeforeEach
  void setUp() throws Exception {
    tradeService = new TradeService(
        Mappers.getMapper(CanonicalTradeMapper.class),
        Mappers.getMapper(PlatformTradeMapper.class),
        kafkaPublisher);

    clearTradeDb();
  }

  @Test
  void processTrades_shouldEmitPlatformTrade_forValidInput() {
    when(kafkaPublisher.sendPlatformTrade(any(PlatformTrade.class)))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

    CanonicalTrade input = CanonicalTrade.builder()
        .account_number("12345678")
        .security_id("ABC123")
        .trade_type("BUY")
        .amount(new BigDecimal("10.50"))
        .build();

    StepVerifier.create(tradeService.processTrades(List.of(input)))
        .assertNext(output -> {
          assertTrue(output.platform_id() != null && !output.platform_id().isBlank());
          assertEquals("****5678", output.trade().account());
          assertEquals("ABC123", output.trade().security());
          assertEquals("B", output.trade().type());
        })
        .verifyComplete();

    verify(kafkaPublisher).sendPlatformTrade(any(PlatformTrade.class));
  }

  @Test
  void processTrades_shouldFilterOutFailedTrade_andNotPublish() {
    CanonicalTrade invalid = CanonicalTrade.builder()
        .account_number("12A4567")
        .security_id("ABC123")
        .trade_type("BUY")
        .amount(new BigDecimal("10"))
        .build();

    StepVerifier.create(tradeService.processTrades(List.of(invalid)))
        .verifyComplete();

    verify(kafkaPublisher, never()).sendPlatformTrade(any(PlatformTrade.class));
  }

  @Test
  void doGet_shouldReturnTradesByStatus_inTimestampDescOrder() throws Exception {
    ConcurrentMap<CanonicalTrade.TradeStatus, List<CanonicalTrade>> db = getTradeDb();
    db.put(CanonicalTrade.TradeStatus.SUCCESS, List.of(
        trade(CanonicalTrade.TradeStatus.SUCCESS, "11112222", "AAA1", Instant.parse("2026-03-15T10:00:00Z")),
        trade(CanonicalTrade.TradeStatus.SUCCESS, "33334444", "BBB2", Instant.parse("2026-03-15T12:00:00Z"))));

    StepVerifier.create(tradeService.doGet(List.of(CanonicalTrade.TradeStatus.SUCCESS)))
        .assertNext(t -> assertEquals("BBB2", t.security_id()))
        .assertNext(t -> assertEquals("AAA1", t.security_id()))
        .verifyComplete();
  }

  @Test
  void doGet_shouldReturnEmpty_forNullOrEmptyFilter() {
    StepVerifier.create(tradeService.doGet(null)).verifyComplete();
    StepVerifier.create(tradeService.doGet(List.of())).verifyComplete();
  }

  @Test
  void parseTrades_shouldParseJsonFile() {
    FilePart jsonPart = mockFilePart("trades.json",
        "[{\"account\":\"12345678\",\"security\":\"IBM123\",\"type\":\"BUY\",\"amount\":100.10}]");

    StepVerifier.create(tradeService.parseTrades(jsonPart))
        .assertNext(trade -> {
          assertEquals("12345678", trade.account_number());
          assertEquals("IBM123", trade.security_id());
          assertEquals("BUY", trade.trade_type());
        })
        .verifyComplete();
  }

  @Test
  void parseTrades_shouldFail_forUnsupportedExtension() {
    FilePart txtPart = Mockito.mock(FilePart.class);
    when(txtPart.filename()).thenReturn("trades.txt");

    StepVerifier.create(tradeService.parseTrades(txtPart))
        .expectErrorSatisfies(error -> {
          assertTrue(error instanceof ResponseStatusException);
          ResponseStatusException ex = (ResponseStatusException) error;
          assertEquals(HttpStatus.UNSUPPORTED_MEDIA_TYPE, ex.getStatusCode());
        })
        .verify();
  }

  @Test
  void parseTrades_shouldFail_forMissingRequiredCsvHeader() {
    FilePart csvPart = mockFilePart("trades.csv",
        "account,security,type\n12345678,IBM123,BUY");

    StepVerifier.create(tradeService.parseTrades(csvPart))
        .expectErrorSatisfies(error -> {
          assertTrue(error instanceof ResponseStatusException);
          ResponseStatusException ex = (ResponseStatusException) error;
          assertEquals(HttpStatus.BAD_REQUEST, ex.getStatusCode());
          assertTrue(ex.getReason() != null && ex.getReason().contains("Missing required CSV header"));
        })
        .verify();
  }

  private FilePart mockFilePart(final String filename, final String payload) {
    FilePart filePart = Mockito.mock(FilePart.class);
    when(filePart.filename()).thenReturn(filename);
    when(filePart.content()).thenReturn(Flux.just(
        DefaultDataBufferFactory.sharedInstance.wrap(payload.getBytes(StandardCharsets.UTF_8))));
    return filePart;
  }

  private CanonicalTrade trade(final CanonicalTrade.TradeStatus status,
                               final String account,
                               final String security,
                               final Instant timestamp) {
    return CanonicalTrade.builder()
        .account_number(account)
        .security_id(security)
        .trade_type("BUY")
        .amount(BigDecimal.ONE)
        .status(status)
        .validation_errors(List.of())
        .timestamp(timestamp)
        .build();
  }

  @SuppressWarnings("unchecked")
  private ConcurrentMap<CanonicalTrade.TradeStatus, List<CanonicalTrade>> getTradeDb() throws Exception {
    Field field = TradeService.class.getDeclaredField("canonicalTradeDB");
    field.setAccessible(true);
    return (ConcurrentMap<CanonicalTrade.TradeStatus, List<CanonicalTrade>>) field.get(null);
  }

  private void clearTradeDb() throws Exception {
    getTradeDb().clear();
  }
}



