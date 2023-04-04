package com.bootcamp.yanki.dto;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.Data;

/**
 * Clase de transferencia de datos para el microservicio de cuentas
 */
@Data
@JsonInclude(Include.NON_NULL)
public class Account {
	private String id;
	private String customerId;
	private Integer typeAccount;
	private String descripTypeAccount;
	private Double amount;
	private Double maintenance;
	private Integer transaction;
	private Integer operationDay;
	private LocalDateTime dateAccount;
	private String NumberAccount;
	private String typeCustomer;
	private Double startAmount;
}
