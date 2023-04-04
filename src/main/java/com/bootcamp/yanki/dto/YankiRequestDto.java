package com.bootcamp.yanki.dto;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Clase de entidad
 */
@Data
@AllArgsConstructor
public class YankiRequestDto {
	private String id;
	private String typeDocument;
	private String NumberDocument;
	private Double amount;
	private String Telephone;
	private LocalDateTime yankiDate;
	private String email;
	private String imei;
	private String idDebit;
}
