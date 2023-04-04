package com.bootcamp.yanki.dto;

import java.time.LocalDateTime;

import lombok.Data;

@Data
public class Transaction {
	private String id;
	private String productType; //AHORRO, C_CORRIENTE, PLAZO_FIJO, CRE_PERSONAL, CRED_EMPRESARIAL, TAR_CRED_PERSONAL, TAR_CRED_EMPRESARIAL
	private String productId;
	private String customerId;
	private String transactionType; //DEPOSITO, RETIRO, PAGO, CONSUMO
	private Double amount;
	private LocalDateTime transactionDate;
	private String customerType;
	private Double balance;

	public String toStringObject() {
        return id + "," + productType + "," +productId+ "," +customerId+ "," +transactionType+ "," +amount+ "," +null+ "," +customerType+ "," +balance;
    }
	
}
