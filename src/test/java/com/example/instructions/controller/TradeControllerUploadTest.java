package com.example.instructions.controller;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.util.MultiValueMap;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.web.reactive.function.BodyInserters;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TradeControllerUploadTest {

  @LocalServerPort
  private int port;

  private WebTestClient webTestClient;

  @BeforeEach
  void setUp() {
    webTestClient = WebTestClient.bindToServer()
        .baseUrl("http://localhost:" + port)
        .build();
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
        .expectBody()
        .jsonPath("$[0].account").isEqualTo("12345678")
        .jsonPath("$[0].security").isEqualTo("IBM123")
        .jsonPath("$[0].type").isEqualTo("BUY")
        .jsonPath("$[1].account").isEqualTo("87654321")
        .jsonPath("$[1].security").isEqualTo("AAP456")
        .jsonPath("$[1].type").isEqualTo("SELL");
  }

  @Test
  void shouldRejectInvalidFieldPatternsOnUpload() {
    final String payload = """
        [
          {"account":"<ACC-1>\t","security":"<IBM, INC>","type":"\"BUY\";","amount":100.10}
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
        .expectBody()
        .jsonPath("$[0].account").isEqualTo("12345678")
        .jsonPath("$[0].security").isEqualTo("IBM123")
        .jsonPath("$[0].type").isEqualTo("BUY")
        .jsonPath("$[1].account").isEqualTo("87654321")
        .jsonPath("$[1].security").isEqualTo("AAP456")
        .jsonPath("$[1].type").isEqualTo("SELL");
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
        .expectBody()
        .jsonPath("$[0].security").isEqualTo("IBM123")
        .jsonPath("$[1].security").isEqualTo("AAP456");
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
}

