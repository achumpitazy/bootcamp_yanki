package com.bootcamp.yanki.clients;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.bootcamp.yanki.clients.config.RestConfig;
import com.bootcamp.yanki.dto.Transaction;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Clase de acceso al microservicio de transacciones
 */
@Service
public class TransactionsRestClient {
    RestConfig restConfig = new RestConfig();
	
	public Flux<Transaction> getAllXProductId(String id) {
		WebClient webClient = WebClient.create("http://localhost:8086");
        return  webClient.get()
                .uri("/transaction/product/"+id)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .retrieve()
                .bodyToFlux(Transaction.class);
	}
	
	public Flux<Transaction> getTransactionByCustomerId(String customerId) {
		WebClient webClient = WebClient.create("http://localhost:8086");
        return  webClient.get()
                .uri("/transaction/customer/"+customerId)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .retrieve()
                .bodyToFlux(Transaction.class);
	}
	
	public Mono<Transaction> createTransaction(Transaction transaction) {
		WebClient webClient = WebClient.create("http://localhost:8086");
        return  webClient.post()
                .uri("/transaction")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body(Mono.just(transaction), Transaction.class)
                .retrieve()
                .bodyToMono(Transaction.class);
	}
}
