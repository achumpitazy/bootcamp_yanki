package com.bootcamp.yanki.dto;

import com.bootcamp.yanki.entity.Yanki;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class YankiResponseDto {
	private String message;
	private Yanki yanki;

}
