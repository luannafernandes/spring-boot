package com.myproj.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
public class KafkaFacade {

    @Autowired
    public Sender sender;


    @RequestMapping(value = "/esb/{string}", method = {RequestMethod.GET},
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE})
    @ResponseBody
    public void sendToKafka(@PathVariable final String string) {
        sender.send(String.format("Sending string -={%s}=- to kafka ", string));
    }

}
