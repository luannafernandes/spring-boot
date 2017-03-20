package com.myproj.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Optional;


public class main extends SpringBootServletInitializer {

    public  void main(String[] args) throws Exception {
        String name;


        Optional<User> user= Optional.ofNullable(null); // Nullable(new User());

        System.out.println(user.get());

    }

    static class User{
        String name;
    }

}