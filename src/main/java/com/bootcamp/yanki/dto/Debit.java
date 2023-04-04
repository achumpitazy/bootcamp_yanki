package com.bootcamp.yanki.dto;

import java.time.LocalDateTime;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Clase de entidad
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Debit {
	private String id;
	private String customerId;
	private String cardNumber;
	private Double amount;
	private String customerType;
	private LocalDateTime debitDate;
	private List<String> accountId;
	private String productType;
}
