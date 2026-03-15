package com.example.instructions.service;

import static com.example.instructions.util.TradeTransformer.ACCOUNT_PATTERN;
import static com.example.instructions.util.TradeTransformer.SECURITY_PATTERN;
import static com.example.instructions.util.TradeTransformer.TYPE_PATTERN;
import static com.example.instructions.util.TradeTransformer.tranform;

import com.example.instructions.mapper.CanonicalTradeMapper;
import com.example.instructions.mapper.PlatformTradeMapper;
import com.example.instructions.model.CanonicalTrade;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
@RequiredArgsConstructor
@Slf4j
public class TradeService {

  private final CanonicalTradeMapper canonicalTradeMapper;
  private final PlatformTradeMapper platformTradeMapper;
  private final KafkaPublisher kafkaPublisher;

  private static final ConcurrentMap<CanonicalTrade.TradeStatus, List<CanonicalTrade>> canonicalTradeCache
      = new ConcurrentHashMap<>();

  public Flux<CanonicalTrade> processTrades(final List<CanonicalTrade> trades) {
    return Flux.fromIterable(trades)
        .flatMap(this::enrichTrade, 5)
        .flatMap(this::doPutTrade, 5)
        .flatMap(this::doTransformPublishTrade, 5);
  }

  private Mono<CanonicalTrade> doTransformPublishTrade(final CanonicalTrade canonicalTrade) {
    return Mono.fromCallable(() -> tranform(platformTradeMapper, canonicalTrade))
        .subscribeOn(Schedulers.boundedElastic())
        .flatMap(kafkaPublisher::sendPlatformTrade)
        .thenReturn(canonicalTrade);
  }

  private Mono<CanonicalTrade> doPutTrade(final CanonicalTrade canonicalTrade) {
    return Mono.just(canonicalTrade)
        .doOnNext(trade ->
          canonicalTradeCache.computeIfAbsent(trade.status(), status -> new CopyOnWriteArrayList<>())
              .add(trade));
  }

  private Mono<CanonicalTrade> enrichTrade(final CanonicalTrade canonicalTrade) {
    return Mono.fromCallable(() -> {
          final CanonicalTrade mapped = canonicalTradeMapper.toCanonicalTrade(canonicalTrade);
          return mapped.validation_errors().isEmpty()
              ? mapped.withStatus(CanonicalTrade.TradeStatus.SUCCESS)
              : mapped.withStatus(CanonicalTrade.TradeStatus.FAILURE);
        })
        .subscribeOn(Schedulers.boundedElastic());
  }

  public Flux<CanonicalTrade> parseTrades(final FilePart filePart) {
    final String fileName = filePart.filename();
    if (!StringUtils.hasText(fileName)) {
      return Flux.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Uploaded file name is missing"));
    }

    final String normalized = fileName.toLowerCase(Locale.ROOT);
    if (!normalized.endsWith(".json") && !normalized.endsWith(".csv")) {
      return Flux.error(new ResponseStatusException(HttpStatus.UNSUPPORTED_MEDIA_TYPE,
          "Only .json or .csv files are supported"));
    }

    return readAllBytes(filePart)
        .flatMapMany(bytes -> Mono.fromCallable(
                () -> normalized.endsWith(".json") ? parseJson(bytes) : parseCsv(bytes))
            .subscribeOn(Schedulers.boundedElastic())
            .flatMapMany(flux -> flux));
  }

  private Mono<byte[]> readAllBytes(final FilePart filePart) {
    return DataBufferUtils.join(filePart.content())
        .handle((dataBuffer, sink) -> {
          try {
            final byte[] bytes = new byte[dataBuffer.readableByteCount()];
            dataBuffer.read(bytes);
            if (bytes.length == 0) {
              sink.error(
                  new ResponseStatusException(HttpStatus.BAD_REQUEST, "Uploaded file is empty"));
              return;
            }
            sink.next(bytes);
          } finally {
            DataBufferUtils.release(dataBuffer);
          }
        });
  }

  private Flux<CanonicalTrade> parseJson(final byte[] bytes) {
    final String payload = new String(bytes, StandardCharsets.UTF_8);
    final JsonParser parser = JsonParserFactory.getJsonParser();
    List<?> rows;

    try {
      rows = parser.parseList(payload);
    } catch (Exception parseArrayError) {
      try {
        rows = List.of(parser.parseMap(payload));
      } catch (Exception parseObjectError) {
        return Flux.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
            "Invalid JSON format. Expected an array or object of trades."));
      }
    }

    final List<CanonicalTrade> trades = new ArrayList<>();
    for (int index = 0; index < rows.size(); index++) {
      final Object row = rows.get(index);
      if (!(row instanceof Map<?, ?> mapRow)) {
        return Flux.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
            "Invalid JSON row at index " + index + ": expected object"));
      }
      trades.add(mapTrade(mapRow, "JSON row " + index));
    }

    return Flux.fromIterable(trades);
  }

  private Flux<CanonicalTrade> parseCsv(final byte[] bytes) {
    final String payload = new String(bytes, StandardCharsets.UTF_8);
    try (CSVParser parser = CSVFormat.DEFAULT.builder()
        .setHeader()
        .setSkipHeaderRecord(true)
        .setIgnoreSurroundingSpaces(true)
        .build()
        .parse(new StringReader(payload))) {

      final Map<String, String> normalizedHeaders = new LinkedHashMap<>();
      for (String header : parser.getHeaderMap().keySet()) {
        normalizedHeaders.put(header.trim().toLowerCase(Locale.ROOT), header);
      }

      final List<String> requiredHeaderGroups = List.of("account_number|account", "security_id|security",
          "trade_type|type", "amount");
      for (String requiredHeaderGroup : requiredHeaderGroups) {
        if (!containsAnyHeader(normalizedHeaders, requiredHeaderGroup.split("\\|"))) {
          return Flux.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
              "Missing required CSV header: " + requiredHeaderGroup.replace('|', '/')));
        }
      }

      final List<CanonicalTrade> trades = new ArrayList<>();
      for (CSVRecord record : parser) {
        final Map<String, Object> rowMap = new LinkedHashMap<>();
        for (Map.Entry<String, String> headerEntry : normalizedHeaders.entrySet()) {
          final String headerName = headerEntry.getValue();
          final String value = record.isMapped(headerName) ? record.get(headerName) : "";
          rowMap.put(headerEntry.getKey(), value);
        }
        trades.add(mapTrade(rowMap, "CSV row " + (record.getRecordNumber() + 1)));
      }

      if (trades.isEmpty()) {
        return Flux.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
            "CSV contains no valid data rows"));
      }

      return Flux.fromIterable(trades);
    } catch (IOException | UncheckedIOException | IllegalArgumentException ex) {
      return Flux.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
          "Invalid CSV format. Ensure rows and quotes are well formed."));
    }
  }

  private CanonicalTrade mapTrade(final Map<?, ?> source, final String rowDescription) {
    final String account = requirePattern(getFirstPresent(source, "account_number", "account"),
        "account_number", rowDescription,
        ACCOUNT_PATTERN, "must be exactly 8 digits");
    final String security = requirePattern(getFirstPresent(source, "security_id", "security"),
        "security_id", rowDescription,
        SECURITY_PATTERN, "must be alphanumeric and can be any length");
    final String type = requirePattern(getFirstPresent(source, "trade_type", "type"),
        "trade_type", rowDescription,
        TYPE_PATTERN, "must be one of BUY, SELL, B, or S");
    final BigDecimal amount = parseAmount(source.get("amount"), rowDescription);

    return CanonicalTrade.builder()
        .account_number(account)
        .security_id(security)
        .trade_type(type)
        .amount(amount)
        .build();
  }

  private boolean containsAnyHeader(final Map<String, String> headers, final String... candidates) {
    for (String candidate : candidates) {
      if (headers.containsKey(candidate)) {
        return true;
      }
    }
    return false;
  }

  private Object getFirstPresent(final Map<?, ?> source, final String... keys) {
    for (String key : keys) {
      if (source.containsKey(key)) {
        return source.get(key);
      }
    }
    return null;
  }

  private String requireText(final Object value, final String fieldName, final String rowDescription) {
    final String text = value == null ? "" : value.toString().trim();
    if (!StringUtils.hasText(text)) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
          "Missing required field '" + fieldName + "' in " + rowDescription);
    }
    return text;
  }

  private String requirePattern(final Object value, final String fieldName, final String rowDescription,
                                final Pattern pattern, final String expectation) {
    final String text = requireText(value, fieldName, rowDescription);
    if (!pattern.matcher(text).matches()) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
          "Invalid field '" + fieldName + "' in " + rowDescription + ": " + expectation);
    }
    return text;
  }

  private BigDecimal parseAmount(final Object value, final String rowDescription) {
    final String text = value == null ? "" : value.toString().trim();
    if (!StringUtils.hasText(text)) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
          "Missing required field 'amount' in " + rowDescription);
    }

    try {
      return new BigDecimal(text);
    } catch (NumberFormatException ex) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
          "Invalid amount in " + rowDescription + ": '" + text + "'");
    }
  }
}

