package com.myproj.kafka;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * Created by dumin on 4/20/17.
 */
//@Configuration
//@EnableAutoConfiguration
//@EnableKafka
//@ComponentScan("com.myproj")
//@Import({ReceiverConfig.class,
//        SenderConfig.class})
@PropertySource("classpath:kafka.properties")
@SpringBootApplication
public class SpringKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaApplication.class, args);
    }
}