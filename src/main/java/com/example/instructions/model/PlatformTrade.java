package com.example.instructions.model;

import lombok.Builder;

@Builder(toBuilder = true)
public record PlatformTrade(String platform_id,
                            CanonicalTrade trade) {
   public static final byte VERSION = 1;
}
