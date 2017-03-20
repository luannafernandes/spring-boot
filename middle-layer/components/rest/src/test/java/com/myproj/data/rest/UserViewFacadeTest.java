package com.myproj.data.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myproj.rest.UserViewFacade;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.util.Optional;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

@RunWith(SpringRunner.class)
@WebMvcTest(UserViewFacade.class)
public class UserViewFacadeTest {

    private static final String GET_USER_URL = "/user/";
    private static final Integer USER_ID = 12;

    @Autowired
    private MockMvc mvc;
//    @Autowired
//    private UserService userService;

    private ProjectionFactory projectionFactory;

    private ObjectMapper mapper;

    @Before
    public void setUp() {
        mapper = new ObjectMapper();
        projectionFactory = new SpelAwareProxyProjectionFactory();
    }

    @Test
    public void getUserByDiShouldReturnUserById() throws Exception {

        MvcResult response = this.mvc
                .perform(MockMvcRequestBuilders.get(GET_USER_URL + USER_ID).accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(jsonPath("$.userId").value("12"))
                .andExpect(jsonPath("$.name").value("name"))
                .andReturn();
        Optional<Long> longOptional = Optional.ofNullable(new Long(12L));

    }

}
