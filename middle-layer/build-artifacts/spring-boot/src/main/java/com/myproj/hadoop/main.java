package com.myproj.hadoop;

import org.springframework.boot.web.support.SpringBootServletInitializer;

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