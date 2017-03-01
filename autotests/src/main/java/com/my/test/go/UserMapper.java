package com.my.test.go;

import org.json.JSONObject;

public interface UserMapper {

    static User map(final JSONObject user) {
        return ImmutableUser.builder()
                .userId(Integer.valueOf(user.getString("userId")))
                .name(user.getString("name"))
                .build();
    }
}
