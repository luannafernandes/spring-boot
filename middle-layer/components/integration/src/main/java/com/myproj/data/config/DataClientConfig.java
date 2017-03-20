package com.myproj.data.config;

import com.myproj.data.services.UserService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class DataClientConfig {

    @Bean
    public UserService restClient(DataProperties dataProperties) {
        return new UserService(dataProperties);
    }

}
