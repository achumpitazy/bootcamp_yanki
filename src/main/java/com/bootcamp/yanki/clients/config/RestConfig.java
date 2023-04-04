package com.bootcamp.yanki.clients.config;

import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.config.WebFluxConfigurer;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Clase de configuración para llamar a otros microservicios
 */
@Configurable
public class RestConfig implements WebFluxConfigurer {

    @Bean
    public WebClient.Builder getWebClient(String url){
        return WebClient.builder().baseUrl(url);

    }
}
