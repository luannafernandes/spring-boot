package com.myproj.data;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.web.client.RestTemplate;

@TestConfiguration
@SpringBootConfiguration
@ComponentScan(basePackages = {"com.myproj"}, excludeFilters = @ComponentScan.Filter(pattern = "com.myproj.config.SpringBootWebApplication", type = FilterType.REGEX))
@PropertySources(value = {@PropertySource("classpath:application-test.properties")})
public class TestConfig {

    @Autowired
    Environment env;

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

}
