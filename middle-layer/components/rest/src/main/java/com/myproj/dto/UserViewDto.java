package com.myproj.dto;

//@Projection(name = "userView", types = { User.class })
public interface UserViewDto {

    String getUserId();

    String getName();

}