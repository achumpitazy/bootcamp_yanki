package com.bootcamp.yanki.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.bootcamp.yanki.dto.YankiRequestDto;
import com.bootcamp.yanki.dto.YankiResponseDto;
import com.bootcamp.yanki.entity.Yanki;
import com.bootcamp.yanki.service.YankiService;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/yanki")
@RequiredArgsConstructor
public class YankiController {
	
	@Autowired
    private YankiService yankiService;

    /**
     *
     * Obtener la información de todas las tarjetas debito
     * @return
     */
    @GetMapping
    public Flux<Yanki> getAll(){
        return yankiService.getAll();
    }

    /**
     *
     * Obtene tarjetas debito por Id
     * @param id
     * @return
     */
    @GetMapping ("/{id}")
    public Mono<Yanki> getYankiById(@PathVariable String id){
        return yankiService.getYankiById(id);
    }

    /**
     * Registrar debit card
     * @param debit
     * @return
     */
    @PostMapping
    public Mono<YankiResponseDto> createYanki(@RequestBody YankiRequestDto yanki){
        return yankiService.createYanki(yanki);
    }

    @DeleteMapping("/{id}")
    public Mono<Yanki> deleteYanki(@PathVariable String id){
        return yankiService.deleteYanki(id);
    }
    
    @GetMapping ("/telephone/{id}")
    public Mono<Yanki> getYankiByTelephone(@PathVariable String id){
        return yankiService.getYankiByTelephone(id);
    }
    
    @PostMapping("/deposit")
    public Mono<YankiResponseDto> depositYanki(@RequestBody YankiRequestDto yanki){
        return yankiService.depositYanki(yanki);
    }
    
    @PostMapping("/pay")
    public Mono<YankiResponseDto> payYanki(@RequestBody YankiRequestDto yanki){
        return yankiService.payYanki(yanki);
    }

}
