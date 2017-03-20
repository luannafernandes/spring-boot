package com.myproj.data.services;


import com.myproj.data.config.DataProperties;
import com.myproj.data.dto.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

//import org.json.JSONArray;

public class UserService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserService.class);

    private DataProperties properties;

    public UserService(DataProperties properties) {
        this.properties = properties;
    }

    //TODO mock
    public Optional<User> findUserById(final Integer userId) {

        LOGGER.info("[findUserById] -> userId: {};", userId);


        return Optional.ofNullable(new User() {
                                       @Override
                                       public Integer getUserId() {
                                           return 12;
                                       }

                                       @Override
                                       public String getName() {
                                           return "name";
                                       }

                                       @Override
                                       public String getAddress() {
                                           return "address";
                                       }
                                   }

        );
//        return Optional.ofNullable(UserMapper.map(project));
    }

}
