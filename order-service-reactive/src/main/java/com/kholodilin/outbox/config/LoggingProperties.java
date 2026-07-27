package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LoggingProperties {

    @Builder.Default
    private Json json = Json.builder().build();

    @Getter
    @Setter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Json {

        @Builder.Default
        private boolean enabled = true;

        @Builder.Default
        private String path = "./logs";
    }
}
