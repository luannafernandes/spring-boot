package com.myproj.rest;

import com.myproj.data.dto.User;
import com.myproj.data.services.UserService;
import com.myproj.dto.UserViewDto;
import com.myproj.kafka.Sender;
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

    @Autowired
    public Sender sender;


    @RequestMapping(value = "/user/{id}", method = {RequestMethod.GET},
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ResponseBody
    public UserViewDto getUser(@PathVariable final Integer id) {
        Optional<User> user = userService.findUserById(id);
        return conversionService.convert(user.get(), UserViewDto.class);
    }

    @RequestMapping(value = "/esb/{string}", method = {RequestMethod.GET},
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ResponseBody
    public void sendToKafka(@PathVariable final String string) {
        sender.send(String.format("Sending string -={%s}=- to kafka ", string));
    }

}
