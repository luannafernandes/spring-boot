package com.my.temp.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootConfiguration
@EnableAutoConfiguration
@EnableConfigurationProperties
@EnableScheduling
@ComponentScan(basePackages = {"com"})
@Import(SwaggerConfig.class)
//@EntityScan(basePackages = { "com.epam.csd.domain" })
//@EnableJpaRepositories("com.epam.csd.services")
//@ImportResource("classpath:securityContext.xml")
public class SpringBootWebApplication extends SpringBootServletInitializer {
    public static void main(String[] args) throws Exception {
        SpringApplication.run(SpringBootWebApplication.class, args);
    }
}