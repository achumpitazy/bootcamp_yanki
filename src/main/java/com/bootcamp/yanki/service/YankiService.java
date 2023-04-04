package com.bootcamp.yanki.service;

import com.bootcamp.yanki.dto.YankiRequestDto;
import com.bootcamp.yanki.dto.YankiResponseDto;
import com.bootcamp.yanki.entity.Yanki;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface YankiService {

    Flux<Yanki> getAll();
    Mono<Yanki> getYankiById(String yankiId);
//    Mono<YankiResponse> saveYanki(Yanki yanki);
//    Mono<Yanki> updateYanki(Yanki yanki);
    Mono<Yanki> deleteYanki(String yankiId);
	Mono<Yanki> getYankiByTelephone(String customerId);
	
//	Mono<YankiResponse> updateYankiDeposit(Yanki debit);
//	
//	Mono<YankiResponse> updateYankiPay(Yanki debit);
	Mono<YankiResponseDto> createYanki(YankiRequestDto wallet);
	
	Mono<YankiResponseDto> depositYanki(YankiRequestDto wallet);
	
	Mono<YankiResponseDto> payYanki(YankiRequestDto wallet);
}
