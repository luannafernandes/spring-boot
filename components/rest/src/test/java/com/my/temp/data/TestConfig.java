package com.my.temp.data;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.web.client.RestTemplate;

@TestConfiguration
@SpringBootConfiguration
@ComponentScan(basePackages = {"com.my.temp"}, excludeFilters = @ComponentScan.Filter(pattern = "com.my.temp.config.SpringBootWebApplication", type = FilterType.REGEX))
@PropertySources(value = {@PropertySource("classpath:application-test.properties")})
public class TestConfig {

    @Autowired
    Environment env;

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

}
