package com.bootcamp.yanki.entity;

import java.time.LocalDateTime;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Clase de entidad
 */
@Data
@AllArgsConstructor
@Document(collection="yanki")
public class Yanki {
	@Id
	private String id;
	private String typeDocument;
	private String numberDocument;
	private Double amount;
	private String telephone;
	private LocalDateTime yankiDate;
	private String email;
	private String imei;
	private String idDebit;
	private String typeProduct;
	private Double startAmount;
}
