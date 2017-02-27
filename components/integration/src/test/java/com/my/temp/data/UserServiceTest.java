package com.my.temp.data;

import com.my.temp.data.dto.User;
import com.my.temp.data.services.UserService;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

//import org.json.JSONArray;
//import org.json.JSONObject;
//import org.springframework.web.client.RestTemplate;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;


public class UserServiceTest {

    @Test
    public void findProjectByEmployeeId() throws Exception {
        UserService userService = new UserService(null);
        Optional<User> user = userService.findUserById(12);

//        assertEquals(12D, user.get().getUserId());
        assertEquals(new Integer(12), user.get().getUserId());
        assertEquals("name", user.get().getName());
        assertEquals("address", user.get().getAddress());
    }

}