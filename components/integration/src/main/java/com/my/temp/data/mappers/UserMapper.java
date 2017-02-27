package com.my.temp.data.mappers;

import com.my.temp.data.dto.ImmutableUser;
import com.my.temp.data.dto.User;
import org.json.JSONObject;

public interface UserMapper {

    static User map(final JSONObject user) {
        return ImmutableUser.builder()
                .userId(Integer.valueOf(user.getString("userId")))
                .name(user.getString("name"))
                .address(user.getString("address"))
                .build();

    }
}
