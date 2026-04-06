package com.tus.ledger.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@EnableKafka
@EnableTransactionManagement
@SpringBootApplication
public class LedgerConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(LedgerConsumerApplication.class, args);
    }
}
