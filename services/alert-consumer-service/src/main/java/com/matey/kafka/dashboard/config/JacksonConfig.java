package com.matey.kafka.dashboard.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {

    /**
     * Shared {@link ObjectMapper} for parsing JSON alert payloads.
     *
     * <p>Registers {@link JavaTimeModule} so Java time types (e.g. {@link java.time.Instant})
     * can be serialized/deserialized consistently.</p>
     */
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }
}
