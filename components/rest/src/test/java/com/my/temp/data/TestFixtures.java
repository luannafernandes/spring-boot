package com.my.temp.data;

import com.my.temp.data.dto.User;

import java.util.Optional;

public class TestFixtures {

    public static Optional<User> createUser() {
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
    }

}
