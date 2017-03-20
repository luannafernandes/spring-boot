package com.myproj.converter;

import com.myproj.data.dto.User;
import com.myproj.dto.UserViewDto;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;

public class UserToViewDtoConverter implements Converter<User, UserViewDto> {

    private final SpelAwareProxyProjectionFactory userFactory;

    public UserToViewDtoConverter(SpelAwareProxyProjectionFactory userFactory) {
        this.userFactory = userFactory;
    }

    @Override
    public UserViewDto convert(User source) {
        return this.userFactory.createProjection(UserViewDto.class, source);
    }
}
