package com.myproj.data.dto;

import com.myproj.data.TestFixtures;
import com.myproj.data.services.UserService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
public class UserViewDtoTest {

    private static final Integer USER_ID = 12;

    private SpelAwareProxyProjectionFactory factory;

    @Mock
    UserService userService;

    @Before
    public void setup() {
        StaticListableBeanFactory beanFactory = new StaticListableBeanFactory();
        beanFactory.addBean("userService", userService);
        factory = new SpelAwareProxyProjectionFactory();
        factory.setBeanFactory(beanFactory);
        Mockito.when(userService.findUserById(USER_ID)).thenReturn(TestFixtures.createUser());
    }

    @Test
    public void testSomething() {
    }
}
