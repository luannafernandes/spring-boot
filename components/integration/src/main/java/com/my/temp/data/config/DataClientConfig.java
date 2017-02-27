package com.my.temp.data.config;

import com.my.temp.data.services.UserService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class DataClientConfig {

    @Bean
    public UserService restClient(DataProperties dataProperties) {
        return new UserService(dataProperties);
    }

}
