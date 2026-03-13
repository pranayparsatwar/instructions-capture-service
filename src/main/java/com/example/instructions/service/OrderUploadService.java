package com.example.instructions.service;

import com.example.instructions.model.Order;
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

@Service
public class OrderUploadService {

  public Flux<Order> parseOrders(final FilePart filePart) {
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
        .flatMapMany(bytes -> normalized.endsWith(".json") ? parseJson(bytes) : parseCsv(bytes));
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

  private Flux<Order> parseJson(final byte[] bytes) {
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
            "Invalid JSON format. Expected an array or object of orders."));
      }
    }

    final List<Order> orders = new ArrayList<>();
    for (int index = 0; index < rows.size(); index++) {
      final Object row = rows.get(index);
      if (!(row instanceof Map<?, ?> mapRow)) {
        return Flux.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
            "Invalid JSON row at index " + index + ": expected object"));
      }
      orders.add(mapOrder(mapRow, "JSON row " + index));
    }

    return Flux.fromIterable(orders);
  }

  private Flux<Order> parseCsv(final byte[] bytes) {
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

      final List<String> requiredHeaders = List.of("account", "security", "type", "amount");
      for (String requiredHeader : requiredHeaders) {
        if (!normalizedHeaders.containsKey(requiredHeader)) {
          return Flux.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
              "Missing required CSV header: " + requiredHeader));
        }
      }

      final List<Order> orders = new ArrayList<>();
      for (CSVRecord record : parser) {
        final Map<String, Object> rowMap = new LinkedHashMap<>();
        for (Map.Entry<String, String> headerEntry : normalizedHeaders.entrySet()) {
          final String headerName = headerEntry.getValue();
          final String value = record.isMapped(headerName) ? record.get(headerName) : "";
          rowMap.put(headerEntry.getKey(), value);
        }
        orders.add(mapOrder(rowMap, "CSV row " + (record.getRecordNumber() + 1)));
      }

      if (orders.isEmpty()) {
        return Flux.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
            "CSV contains no valid data rows"));
      }

      return Flux.fromIterable(orders);
    } catch (IOException | UncheckedIOException | IllegalArgumentException ex) {
      return Flux.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
          "Invalid CSV format. Ensure rows and quotes are well formed."));
    }
  }

  private Order mapOrder(final Map<?, ?> source, final String rowDescription) {
    final String account = requireText(source.get("account"), "account", rowDescription);
    final String security = requireText(source.get("security"), "security", rowDescription);
    final String type = requireText(source.get("type"), "type", rowDescription);
    final BigDecimal amount = parseAmount(source.get("amount"), rowDescription);

    return Order.builder()
        .account(account)
        .security(security)
        .type(type)
        .amount(amount)
        .build();
  }

  private String requireText(final Object value, final String fieldName, final String rowDescription) {
    final String text = value == null ? "" : value.toString().trim();
    if (!StringUtils.hasText(text)) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
          "Missing required field '" + fieldName + "' in " + rowDescription);
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

