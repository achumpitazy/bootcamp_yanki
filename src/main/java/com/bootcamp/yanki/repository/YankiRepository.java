package com.bootcamp.yanki.repository;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

import com.bootcamp.yanki.entity.Yanki;

/**
 * Clase Repositorio para los métodos de acceso a la base de datos
 */
public interface YankiRepository extends ReactiveMongoRepository<Yanki, String> {
	
}
