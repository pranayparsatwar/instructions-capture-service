package com.example.instructions.controller;

import com.example.instructions.model.Order;
import com.example.instructions.service.OrderUploadService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
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

  private final OrderUploadService orderUploadService;

  @PostMapping(path = "/capture",
      consumes = MediaType.APPLICATION_NDJSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public Flux<Order> capture(final @RequestBody List<Order> order) {
    log.info("Received order: {}", order);
    return Flux.fromIterable(order);
  }

  @PostMapping(path = "/capture/upload",
      consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public Flux<Order> captureUpload(final @RequestPart("file") Mono<FilePart> filePartMono) {
    return filePartMono.flatMapMany(orderUploadService::parseOrders);
  }
}
