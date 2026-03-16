package com.example.instructions.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.instructions.model.PlatformTrade;
import com.example.instructions.service.KafkaPublisher;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.util.MultiValueMap;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.web.reactive.function.BodyInserters;
import reactor.core.publisher.Mono;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TradeControllerUploadTest {

  @MockitoBean
  private KafkaPublisher kafkaPublisher;

  @LocalServerPort
  private int port;

  private WebTestClient webTestClient;

  @BeforeEach
  void setUp() {
    webTestClient = WebTestClient.bindToServer()
        .baseUrl("http://localhost:" + port)
        .build();

    when(kafkaPublisher.sendPlatformTrade(any(PlatformTrade.class)))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
  }

  @Test
  void shouldUploadAndParseJsonTrades() {
    final String payload = """
        [
          {"account":"12345678","security":"IBM123","type":"BUY","amount":100.10},
          {"account":"87654321","security":"AAP456","type":"SELL","amount":50}
        ]
        """;

    webTestClient.post()
        .uri("/api/trade/v1/instructions/capture")
        .contentType(MediaType.MULTIPART_FORM_DATA)
        .body(BodyInserters.fromMultipartData(singleFilePart("trades.json", MediaType.APPLICATION_JSON_VALUE, payload)))
        .exchange()
        .expectStatus().isOk()
        .expectBodyList(PlatformTrade.class)
        .value(this::assertExpectedTwoTrades);
  }

  @Test
  void shouldRejectInvalidFieldPatternsOnUpload() {
    final String payload = """
        [
          {"account":"<ACC-1>\t","security":"<IBM, INC>","type":""BUY";","amount":100.10}
        ]
        """;

    webTestClient.post()
        .uri("/api/trade/v1/instructions/capture")
        .contentType(MediaType.MULTIPART_FORM_DATA)
        .body(BodyInserters.fromMultipartData(singleFilePart("trades.json", MediaType.APPLICATION_JSON_VALUE, payload)))
        .exchange()
        .expectStatus().isBadRequest();
  }

  @Test
  void shouldUploadAndParseCsvTrades() {
    final String payload = """
        account,security,type,amount
        12345678,IBM123,BUY,100.10
        87654321,AAP456,SELL,50
        """;

    webTestClient.post()
        .uri("/api/trade/v1/instructions/capture")
        .contentType(MediaType.MULTIPART_FORM_DATA)
        .body(BodyInserters.fromMultipartData(singleFilePart("trades.csv", "text/csv", payload)))
        .exchange()
        .expectStatus().isOk()
        .expectBodyList(PlatformTrade.class)
        .value(this::assertExpectedTwoTrades);
  }

  @Test
  void shouldUploadAndParseQuotedCsvValues() {
    final String payload = """
        account,security,type,amount,note
        12345678,IBM123,BUY,100.10,"IBM, INC"
        87654321,AAP456,SELL,50,"AAPL, CLASS A"
        """;

    webTestClient.post()
        .uri("/api/trade/v1/instructions/capture")
        .contentType(MediaType.MULTIPART_FORM_DATA)
        .body(BodyInserters.fromMultipartData(singleFilePart("trades.csv", "text/csv", payload)))
        .exchange()
        .expectStatus().isOk()
        .expectBodyList(PlatformTrade.class)
        .value(trades -> {
          assertEquals(2, trades.size());
          Set<String> securities = trades.stream()
              .map(t -> t.trade().security())
              .collect(Collectors.toSet());
          assertEquals(Set.of("IBM123", "AAP456"), securities);
        });
  }

  @Test
  void shouldRejectMalformedQuotedCsv() {
    final String payload = """
        account,security,type,amount
        ACC-1,"IBM, INC,BUY,100.10
        """;

    webTestClient.post()
        .uri("/api/trade/v1/instructions/capture")
        .contentType(MediaType.MULTIPART_FORM_DATA)
        .body(BodyInserters.fromMultipartData(singleFilePart("trades.csv", "text/csv", payload)))
        .exchange()
        .expectStatus().isBadRequest();
  }

  @Test
  void shouldRejectUnsupportedExtension() {
    webTestClient.post()
        .uri("/api/trade/v1/instructions/capture")
        .contentType(MediaType.MULTIPART_FORM_DATA)
        .body(BodyInserters.fromMultipartData(singleFilePart("trades.txt", MediaType.TEXT_PLAIN_VALUE,
            "account,security,type,amount\n12345678,IBM123,BUY,100")))
        .exchange()
        .expectStatus().isEqualTo(415);
  }

  private MultiValueMap<String, ?> singleFilePart(final String fileName, final String contentType,
                                                   final String payload) {
    final ByteArrayResource resource = new ByteArrayResource(payload.getBytes(StandardCharsets.UTF_8)) {
      @Override
      public String getFilename() {
        return fileName;
      }
    };

    final MultipartBodyBuilder multipartBodyBuilder = new MultipartBodyBuilder();
    multipartBodyBuilder.part("file", resource)
        .contentType(MediaType.parseMediaType(contentType));
    return multipartBodyBuilder.build();
  }

  private void assertExpectedTwoTrades(final List<PlatformTrade> trades) {
    assertEquals(2, trades.size());
    assertTrue(trades.stream().allMatch(trade -> trade.platform_id() != null && !trade.platform_id().isBlank()));

    Set<String> accounts = trades.stream()
        .map(trade -> trade.trade().account())
        .collect(Collectors.toSet());
    assertEquals(Set.of("****5678", "****4321"), accounts);

    Set<String> securities = trades.stream()
        .map(trade -> trade.trade().security())
        .collect(Collectors.toSet());
    assertEquals(Set.of("IBM123", "AAP456"), securities);

    Set<String> types = trades.stream()
        .map(trade -> trade.trade().type())
        .collect(Collectors.toSet());
    assertEquals(Set.of("B", "S"), types);
  }
}

