package com.example.instructions.service;

import static com.example.instructions.util.TradeTransformer.ACCOUNT_PATTERN;
import static com.example.instructions.util.TradeTransformer.SECURITY_PATTERN;
import static com.example.instructions.util.TradeTransformer.TYPE_PATTERN;
import static com.example.instructions.util.TradeTransformer.tranform;

import com.example.instructions.mapper.CanonicalTradeMapper;
import com.example.instructions.mapper.PlatformTradeMapper;
import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.CanonicalTrade.TradeStatus;
import com.example.instructions.model.PlatformTrade;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
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

  private static final ConcurrentMap<CanonicalTrade.TradeStatus, List<CanonicalTrade>> canonicalTradeDB
      = new ConcurrentHashMap<>();

  public Flux<CanonicalTrade> doGet(List<CanonicalTrade.TradeStatus> statusFilter) {
    if (statusFilter == null || statusFilter.isEmpty()) {
      return Flux.empty();
    }

    return Flux.fromIterable(statusFilter)
        .concatMap(status -> {
          final List<CanonicalTrade> sorted = canonicalTradeDB
              .getOrDefault(status, List.of())
              .stream()
              .sorted(CanonicalTrade.TIMESTAMP_DESC_COMPARATOR)
              .toList();

          return Flux.fromIterable(sorted);
        });
  }

  public Flux<PlatformTrade> processTrades(final List<CanonicalTrade> trades) {
    return Flux.fromIterable(trades)
        .doOnSubscribe(subscription -> log.debug("Processing trade batch tradeCount={}", trades.size()))
        .flatMap(this::enrichTrade, 5)
        .flatMap(this::doPutTrade, 5)
        .filter(trade -> trade.status() == TradeStatus.SUCCESS)
        .flatMap(this::doTransformPublishTrade, 5);
  }

  private Mono<PlatformTrade> doTransformPublishTrade(final CanonicalTrade canonicalTrade) {
    return Mono.fromCallable(() -> tranform(platformTradeMapper, canonicalTrade))
        .subscribeOn(Schedulers.boundedElastic())
        .doOnNext(platformTrade -> log.debug(
            "Prepared PlatformTrade output platformId={} security={} inputSecurityId={}",
            platformTrade.platform_id(),
            platformTrade.trade() == null ? null : platformTrade.trade().security(),
            canonicalTrade.security_id()))
        .flatMap(kafkaPublisher::sendPlatformTrade);
  }

  private Mono<CanonicalTrade> doPutTrade(final CanonicalTrade canonicalTrade) {
    return Mono.just(canonicalTrade)
        .doOnNext(trade ->
          canonicalTradeDB.computeIfAbsent(trade.status(), status -> new CopyOnWriteArrayList<>())
              .add(trade));
  }

  private Mono<CanonicalTrade> enrichTrade(final CanonicalTrade canonicalTrade) {
    return Mono.fromCallable(() -> {
          final CanonicalTrade mapped = canonicalTradeMapper.toCanonicalTrade(canonicalTrade);
          return mapped.validation_errors().isEmpty()
              ? mapped.withStatus(CanonicalTrade.TradeStatus.SUCCESS)
              : mapped.withStatus(CanonicalTrade.TradeStatus.FAILED);
        })
        .doOnNext(trade -> log.debug(
            "Enriched CanonicalTrade accountNumber={} securityId={} status={} validationErrorCount={}",
            trade.account_number(),
            trade.security_id(),
            trade.status(),
            trade.validation_errors() == null ? 0 : trade.validation_errors().size()))
        .subscribeOn(Schedulers.boundedElastic());
  }

  public Flux<CanonicalTrade> parseTrades(final FilePart filePart) {
    final String fileName = filePart.filename();
    if (!StringUtils.hasText(fileName)) {
      return Flux.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Uploaded file name is missing"));
    }

    final String normalized = fileName.toLowerCase(Locale.ROOT);
    if (normalized.endsWith(".json")) {
      return parseJsonStreaming(filePart);
    }
    if (normalized.endsWith(".csv")) {
      return parseCsvStreaming(filePart);
    }

    return Flux.error(new ResponseStatusException(
        HttpStatus.UNSUPPORTED_MEDIA_TYPE, "Only .json or .csv files are supported"));
  }

  private Flux<CanonicalTrade> parseJsonStreaming(final FilePart filePart) {
    return Flux.using(
            () -> DataBufferUtils.subscriberInputStream(filePart.content(), 16),
            input -> Flux.<CanonicalTrade>create(sink -> {
              final JsonFactory jsonFactory = new JsonFactory();
              final ObjectMapper objectMapper = new ObjectMapper(jsonFactory);

              try (var parser = jsonFactory.createParser(input)) {
                final JsonToken firstToken = parser.nextToken();
                if (firstToken == null) {
                  sink.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Uploaded file is empty"));
                  return;
                }

                if (firstToken == JsonToken.START_ARRAY) {
                  int index = 0;
                  while (parser.nextToken() != JsonToken.END_ARRAY) {
                    if (sink.isCancelled()) {
                      return;
                    }
                    if (parser.currentToken() != JsonToken.START_OBJECT) {
                      sink.error(new ResponseStatusException(
                          HttpStatus.BAD_REQUEST,
                          "Invalid JSON row at index " + index + ": expected object"));
                      return;
                    }

                    final Map<String, Object> row = objectMapper.readValue(
                        parser, new TypeReference<>() {});
                    sink.next(mapTrade(row, "JSON row " + index));
                    index++;
                  }

                  if (index == 0) {
                    sink.error(new ResponseStatusException(
                        HttpStatus.BAD_REQUEST, "JSON contains no valid data rows"));
                    return;
                  }

                  sink.complete();
                  return;
                }

                if (firstToken == JsonToken.START_OBJECT) {
                  final Map<String, Object> row = objectMapper.readValue(
                      parser, new TypeReference<>() {});
                  sink.next(mapTrade(row, "JSON row 0"));
                  sink.complete();
                  return;
                }

                sink.error(new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Invalid JSON format. Expected an array or object of trades."));
              } catch (ResponseStatusException ex) {
                sink.error(ex);
              } catch (Exception ex) {
                sink.error(new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Invalid JSON format. Expected an array or object of trades."));
              }
            }),
            this::closeQuietly)
        .subscribeOn(Schedulers.boundedElastic());
  }

  private Flux<CanonicalTrade> parseCsvStreaming(final FilePart filePart) {
    return Flux.using(
            () -> DataBufferUtils.subscriberInputStream(filePart.content(), 16),
            input -> Flux.<CanonicalTrade>create(sink -> {
              try (InputStream in = input;
                  BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
                  CSVParser parser = CSVFormat.DEFAULT.builder()
                      .setHeader()
                      .setSkipHeaderRecord(true)
                      .setIgnoreSurroundingSpaces(true)
                      .build()
                      .parse(reader)) {

                final Map<String, String> normalizedHeaders = new LinkedHashMap<>();
                for (String header : parser.getHeaderMap().keySet()) {
                  normalizedHeaders.put(header.trim().toLowerCase(Locale.ROOT), header);
                }

                final List<String> requiredHeaderGroups = List.of(
                    "account_number|account", "security_id|security", "trade_type|type", "amount");
                for (String requiredHeaderGroup : requiredHeaderGroups) {
                  if (!containsAnyHeader(normalizedHeaders, requiredHeaderGroup.split("\\|"))) {
                    sink.error(new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        "Missing required CSV header: " + requiredHeaderGroup.replace('|', '/')));
                    return;
                  }
                }

                boolean emitted = false;
                for (CSVRecord record : parser) {
                  if (sink.isCancelled()) {
                    return;
                  }

                  final Map<String, Object> rowMap = new LinkedHashMap<>();
                  for (Map.Entry<String, String> headerEntry : normalizedHeaders.entrySet()) {
                    final String headerName = headerEntry.getValue();
                    final String value = record.isMapped(headerName) ? record.get(headerName) : "";
                    rowMap.put(headerEntry.getKey(), value);
                  }

                  sink.next(mapTrade(rowMap, "CSV row " + (record.getRecordNumber() + 1)));
                  emitted = true;
                }

                if (!emitted) {
                  sink.error(new ResponseStatusException(
                      HttpStatus.BAD_REQUEST, "CSV contains no valid data rows"));
                  return;
                }

                sink.complete();
              } catch (ResponseStatusException ex) {
                sink.error(ex);
              } catch (IOException | UncheckedIOException | IllegalArgumentException ex) {
                sink.error(new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Invalid CSV format. Ensure rows and quotes are well formed."));
              }
            }),
            this::closeQuietly)
        .subscribeOn(Schedulers.boundedElastic());
  }

  private void closeQuietly(final InputStream input) {
    try {
      input.close();
    } catch (IOException ignored) {
      // no-op
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

