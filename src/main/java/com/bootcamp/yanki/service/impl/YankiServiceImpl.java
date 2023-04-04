package com.bootcamp.yanki.service.impl;

import java.time.LocalDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.bootcamp.yanki.clients.AccountsRestClient;
import com.bootcamp.yanki.clients.DebitRestClient;
import com.bootcamp.yanki.dto.Account;
import com.bootcamp.yanki.dto.Debit;
import com.bootcamp.yanki.dto.Transaction;
import com.bootcamp.yanki.dto.YankiRequestDto;
import com.bootcamp.yanki.dto.YankiResponseDto;
import com.bootcamp.yanki.entity.Yanki;
import com.bootcamp.yanki.producer.KafkaStringProducer;
import com.bootcamp.yanki.repository.YankiRepository;
import com.bootcamp.yanki.service.YankiService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class YankiServiceImpl implements YankiService{
	@Autowired
	private KafkaStringProducer kafkaStringProducer;
	
    @Autowired
    private YankiRepository yankiRepository;
    
    @Autowired
    private DebitRestClient debitRestClient;
    
    @Autowired
    private AccountsRestClient accountsRestClient;
    
//    @Autowired
//    AccountsRestClient accountsRestClient;
//    
//    @Autowired
//    TransactionsRestClient transactionsRestClient;

    @Override
    public Flux<Yanki> getAll() {
        return yankiRepository.findAll();
    }

    @Override
    public Mono<Yanki> getYankiById(String debitId) {
        return yankiRepository.findById(debitId);
    }

//    @Override
//    public Mono<YankiResponse> saveYanki(Yanki debit) {
//    	YankiResponse response = new YankiResponse();
//		response.setMessage("Error creating debit card");
//		return accountsRestClient.getAccountById(debit.getAccountId().get(0)).flatMap(c -> {
//			debit.setAmount(c.getAmount());
//			debit.setDebitDate(LocalDateTime.now());
//			return yankiRepository.save(debit).flatMap(d -> {
//				response.setDebit(d);
//				response.setMessage("Debit card created");
//				return Mono.just(response);
//			});
//		}).defaultIfEmpty(new YankiResponse("account to associate does not exist",null));
//    }
//
//    @Override
//    public Mono<Yanki> updateYanki(Yanki debit) {
//        return yankiRepository.findById(debit.getId())
//                .flatMap(newDebit -> {
//                    newDebit.setId(debit.getId());
//                    newDebit.setCustomerId(debit.getCustomerId());
//                    newDebit.setAmount(debit.getAmount());
//                    newDebit.setCardNumber(debit.getCardNumber());
//                    newDebit.setDebitDate(debit.getDebitDate());
//                    newDebit.setAccountId(debit.getAccountId());
//                    return yankiRepository.save(newDebit);
//        });
//    }

    @Override
    public Mono<Yanki> deleteYanki(String debitId) {
        return yankiRepository.findById(debitId)
                .flatMap(debit -> yankiRepository.delete(debit)
                        .then(Mono.just(debit)));
    }
    
    @Override
    public Mono<Yanki> getYankiByTelephone(String telephone){
        return yankiRepository.findAll()
        		.filter(yanki -> yanki.getTelephone().equals(telephone)).next();
    }
    
    @Override
	public Mono<YankiResponseDto> createYanki(YankiRequestDto yankiRequestDto) {
    	
    	Yanki yanki = new Yanki(null, yankiRequestDto.getTypeDocument(), yankiRequestDto.getNumberDocument()
    			, yankiRequestDto.getAmount(), yankiRequestDto.getTelephone(), LocalDateTime.now(), yankiRequestDto.getEmail()
    			, yankiRequestDto.getImei(), yankiRequestDto.getIdDebit(), "YANKI", 0.0);
    	
    	YankiResponseDto response = new YankiResponseDto();
		response.setMessage("Error creating debit card");
		response.setYanki(yanki);
		if(yanki.getIdDebit()!=null) {
			response.setMessage("Debit card does not exist");
			yanki.setAmount(0.0);
			response.setYanki(yanki);
			return debitRestClient.getDebitById(yanki.getIdDebit()).flatMap(deb -> {
				yanki.setStartAmount(deb.getAmount());
				yanki.setAmount(deb.getAmount());
				return yankiRepository.save(yanki).flatMap(w -> {
					response.setMessage("Yanki created");
					return Mono.just(response);
				});
			}).defaultIfEmpty(response);
			
		}else {
			yanki.setAmount(0.0);
			return yankiRepository.save(yanki).flatMap(w -> {
				response.setMessage("Yanki created");
				return Mono.just(response);
			});
		}
	}

	@Override
	public Mono<YankiResponseDto> depositYanki(YankiRequestDto yankiRequestDto) {
		Yanki yanki = new Yanki(yankiRequestDto.getId(), yankiRequestDto.getTypeDocument(), yankiRequestDto.getNumberDocument()
    			, yankiRequestDto.getAmount(), yankiRequestDto.getTelephone(), LocalDateTime.now(), yankiRequestDto.getEmail()
    			, yankiRequestDto.getImei(), yankiRequestDto.getIdDebit(), "YANKI", 0.0);
		YankiResponseDto response = new YankiResponseDto();
		response.setMessage("Yanki not exist");
		response.setYanki(yanki);
		return yankiRepository.findAll().filter(y -> y.getTelephone().equals(yankiRequestDto.getTelephone())).next().flatMap(w -> {
			response.setYanki(w);
			w.setAmount(w.getAmount() + yankiRequestDto.getAmount());
			w.setStartAmount(w.getStartAmount());
			if(w.getIdDebit()!=null) {
				return debitRestClient.getDebitById(w.getIdDebit()).flatMap(deb -> {
					w.setIdDebit(deb.getId());
					deb.setAmount(deb.getAmount() + yankiRequestDto.getAmount());
					return debitRestClient.updateDebit(deb).flatMap(upd -> {
						return accountsRestClient.getAccountById(upd.getAccountId().get(0)).flatMap(account -> {
							account.setAmount(account.getAmount() + yankiRequestDto.getAmount());
							return accountsRestClient.updateAccount(account).flatMap(x -> {
								response.setMessage("successful Deposit");
								return yankiRepository.save(w).flatMap(w1 -> {
									response.setYanki(yanki);
									return registerTransaction(w, yankiRequestDto.getAmount(), "DEPOSITO", x, upd);
								});
							});
						});
					});
				});
			}else {
				return yankiRepository.save(w).flatMap(w1 -> {
					response.setYanki(w1);
					response.setMessage("successful Deposit");
					return registerTransaction(w, yankiRequestDto.getAmount(), "DEPOSITO", null, null);
				});
			}			
		}).defaultIfEmpty(response);
	}
	
	@Override
	public Mono<YankiResponseDto> payYanki(YankiRequestDto yankiRequestDto) {
		Yanki yanki = new Yanki(yankiRequestDto.getId(), yankiRequestDto.getTypeDocument(), yankiRequestDto.getNumberDocument()
    			, yankiRequestDto.getAmount(), yankiRequestDto.getTelephone(), LocalDateTime.now(), yankiRequestDto.getEmail()
    			, yankiRequestDto.getImei(), yankiRequestDto.getIdDebit(), "YANKI", 0.0);
		YankiResponseDto response = new YankiResponseDto();
		response.setMessage("Yanki not exist");
		response.setYanki(yanki);
		return yankiRepository.findAll().filter(y -> y.getTelephone().equals(yankiRequestDto.getTelephone())).next().flatMap(w -> {
//		return yankiRepository.findById(yanki.getId()).flatMap(w -> {
			response.setYanki(w);
			w.setStartAmount(w.getStartAmount());
			Double newAmount = w.getAmount() - yankiRequestDto.getAmount();
			if(newAmount<0) {
				response.setMessage("You don't have enough balance");
				return Mono.just(response);
			}
			w.setAmount(w.getAmount() - yankiRequestDto.getAmount());
			if(w.getIdDebit()!=null) {
				return debitRestClient.getDebitById(w.getIdDebit()).flatMap(deb -> {
					w.setIdDebit(deb.getId());
					deb.setAmount(deb.getAmount() - yankiRequestDto.getAmount());
					return debitRestClient.updateDebit(deb).flatMap(upd -> {
						return accountsRestClient.getAccountById(upd.getAccountId().get(0)).flatMap(account -> {
							account.setAmount(account.getAmount() - yankiRequestDto.getAmount());
							return accountsRestClient.updateAccount(account).flatMap(x -> {
								response.setMessage("successful Pay");
								return yankiRepository.save(w).flatMap(w1 -> {
									response.setYanki(w);
									return registerTransaction(w, yankiRequestDto.getAmount(), "RETIRO", x, upd);
								});
							});
						});
					});
				});
			}else {
				return yankiRepository.save(w).flatMap(w1 -> {
					response.setYanki(w);
					response.setMessage("successful Pay");
					return registerTransaction(w, yankiRequestDto.getAmount(), "RETIRO", null, null);
				});
			}			
		}).defaultIfEmpty(response);
	}
	
	private Mono<YankiResponseDto> registerTransaction(Yanki yanki, Double amount, String typeTransaction, Account account, Debit debit){
		Transaction transaction = new Transaction();
		transaction.setCustomerId(yanki.getTelephone());
		transaction.setProductId(yanki.getId());
		transaction.setProductType(yanki.getTypeProduct());
		transaction.setTransactionType(typeTransaction);
		transaction.setAmount(amount);
		transaction.setCustomerType("PERSONAL");
		transaction.setBalance(yanki.getAmount());
		kafkaStringProducer.sendMessage(transaction.toStringObject());
		
		if(yanki.getIdDebit()!=null) {
			transaction.setProductId(account.getId());
			transaction.setCustomerId(account.getCustomerId());
			transaction.setProductType(account.getDescripTypeAccount());
			kafkaStringProducer.sendMessage(transaction.toStringObject());
			transaction.setProductId(debit.getId());
			transaction.setProductType(debit.getProductType());
			kafkaStringProducer.sendMessage(transaction.toStringObject());
		}
		
		return Mono.just(new YankiResponseDto("Successful transaction", yanki));
	}

//	@Override
//	public Mono<YankiResponse> updateYankiDeposit(Yanki debit) {
//		YankiResponse response = new YankiResponse();
//		return yankiRepository.findById(debit.getId()).flatMap(deb -> {
//			Double mount=deb.getAmount() + debit.getAmount();
//			deb.setAmount(mount);
//			return yankiRepository.save(deb).flatMap(upd -> {
//				return accountsRestClient.getAccountById(upd.getAccountId().get(0)).flatMap(account -> {
//					account.setAmount(account.getAmount() + debit.getAmount());
//					return accountsRestClient.updateAccount(account).flatMap(x -> {
//						response.setMessage("successful Deposit");
//						response.setDebit(upd);
//						return registerTransaction(upd, debit.getAmount(), "DEPOSITO", x);
//					});
//				});
//			});
//		}).defaultIfEmpty(new YankiResponse("Debit Card no exist", null));
//	}
//	
//	@Override
//	public Mono<YankiResponse> updateYankiPay(Yanki debit) {
//		YankiResponse response = new YankiResponse();
//		return yankiRepository.findById(debit.getId()).flatMap(deb -> {
//			String idAcc = deb.getAccountId().get(0);
//			response.setMessage("You don't have enough balance");
//			response.setDebit(deb);
//			return getAccountMount(deb,debit).flatMap(b -> {
//				Double newMountDebit = deb.getAmount() - debit.getAmount();
//				Double newMount = b.getAmount()-debit.getAmount();
//				b.setAmount(newMount);
//				return accountsRestClient.updateAccount(b).flatMap(x -> {
//					response.setMessage("successful Pay");
//					response.setDebit(deb);
//					if(idAcc.equals(x.getId())) {
//						deb.setAmount(newMountDebit);
//						return yankiRepository.save(deb).flatMap(upd -> {
//							return registerTransaction(upd, debit.getAmount(), "RETIRO", x);
//						});
//					}
//					return registerTransaction(deb, debit.getAmount(), "RETIRO", x);
//				});
//			}).defaultIfEmpty(response);
//		});
//	}
//	
//	private Mono<YankiResponse> registerTransaction(Yanki debit, Double amount, String typeTransaction, Account accountt){
//		Transaction transaction = new Transaction();
//		transaction.setCustomerId(debit.getCustomerId());
//		transaction.setProductId(debit.getId());
//		transaction.setProductType(debit.getProductType());
//		transaction.setTransactionType(typeTransaction);
//		transaction.setAmount(amount);
//		transaction.setCustomerType(debit.getCustomerType());
//		transaction.setBalance(debit.getAmount());
//		kafkaStringProducer.sendMessage(transaction.toStringObject());
//		transaction.setProductId(accountt.getId());
//		transaction.setProductType(accountt.getDescripTypeAccount());
//		kafkaStringProducer.sendMessage(transaction.toStringObject());
//		return Mono.just(new YankiResponse("Successful transaction", debit));
//	}
//	
//	private Mono<Account> getAccountMount(Yanki debit,Yanki debitNew) {
//        return accountsRestClient.getAllAccountXCustomerId(debit.getCustomerId())
//                .filter(d -> debit.getAccountId().contains(d.getId()))
//                .filter(d -> d.getAmount() >= debitNew.getAmount())
//                .sort((p1, p2) -> Integer.compare(debit.getAccountId().indexOf(p1.getId()), debit.getAccountId().indexOf(p2.getId())))
//                .next();
//    }

	
    
    
}
