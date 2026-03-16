package com.example.instructions.controller;

import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.PlatformTrade;
import com.example.instructions.service.TradeService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/trade/v1/instructions")
@RequiredArgsConstructor
@Slf4j
public class TradeController {

  private final TradeService tradeService;

  @PostMapping(
      consumes = MediaType.APPLICATION_NDJSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public Flux<PlatformTrade> capture(final @RequestBody List<CanonicalTrade> trade) {
    log.info("Received trade: {}", trade);
    return tradeService.processTrades(trade);
  }

  @PostMapping(
      path = "/capture",
      consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public Flux<PlatformTrade> captureUpload(final @RequestPart("file") Mono<FilePart> filePartMono) {
    return filePartMono.flatMapMany(tradeService::parseTrades)
        .buffer(25)
        .flatMap(tradeService::processTrades, 5);
  }

  @GetMapping(
      path = "/history",
      produces = MediaType.APPLICATION_JSON_VALUE)
  public Flux<CanonicalTrade> getAll() {
    return tradeService
        .doGet(List.of(CanonicalTrade.TradeStatus.SUCCESS, CanonicalTrade.TradeStatus.FAILED));
  }

  @GetMapping(
      path = "/history/{status}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  public Flux<CanonicalTrade> get(
      final @PathVariable CanonicalTrade.TradeStatus status) {
    return switch (status) {
      case SUCCESS -> tradeService.doGet(List.of(CanonicalTrade.TradeStatus.SUCCESS));
      case FAILED -> tradeService.doGet(List.of(CanonicalTrade.TradeStatus.FAILED));
    };
  }
}
