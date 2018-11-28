package com.dgtz.api.boot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * BroCast.
 * Copyright: Sardor Navruzov
 * 2013-2016.
 */

@SpringBootApplication
public class InitAPI implements HealthIndicator {
    private static final Logger log = LoggerFactory.getLogger(InitAPI.class);


    @Override
    public Health health() {
        return Health.up().withDetail("api", "core").build();
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(InitAPI.class, args);
    }
}
