package com.example.instructions.util;

import com.example.instructions.mapper.PlatformTradeMapper;
import com.example.instructions.model.CanonicalTrade;
import com.example.instructions.model.PlatformTrade;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;

@UtilityClass
public class TradeTransformer {

  public static final Pattern ACCOUNT_PATTERN = Pattern.compile("^\\d{8}$");
  public static final Pattern SECURITY_PATTERN = Pattern.compile("^[A-Za-z0-9]+$");
  public static final Pattern TYPE_PATTERN = Pattern.compile("^(BUY|SELL|B|S)$", Pattern.CASE_INSENSITIVE);

  public static PlatformTrade tranform(
      final PlatformTradeMapper platformTradeMapper,
      final CanonicalTrade canonicalTrade) {
    return platformTradeMapper.toPlatformTrade(canonicalTrade);
  }

}
