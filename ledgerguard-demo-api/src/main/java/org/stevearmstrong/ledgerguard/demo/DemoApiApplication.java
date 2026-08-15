package org.stevearmstrong.ledgerguard.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.annotation.EnableKafka;
import org.stevearmstrong.ledgerguard.demo.config.LedgerGuardTopics;

@EnableKafka
@SpringBootApplication
@EnableConfigurationProperties(LedgerGuardTopics.class)
public class DemoApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApiApplication.class, args);
    }
}
