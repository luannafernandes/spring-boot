package com.my.temp.rest;

import com.my.temp.data.dto.User;
import com.my.temp.data.services.UserService;
import com.my.temp.dto.UserViewDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.ConversionService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
public class UserViewFacade {

    @Autowired
    private ConversionService conversionService;

    @Autowired
    public UserService userService;


    @RequestMapping(value = "/user/{id}", method = {RequestMethod.GET},
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ResponseBody
    public UserViewDto getUser(@PathVariable final Integer id) {
        Optional<User> user = userService.findUserById(id);
        return conversionService.convert(user.get(), UserViewDto.class);
    }

}
