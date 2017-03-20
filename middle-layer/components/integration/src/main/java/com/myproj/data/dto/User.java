package com.myproj.data.dto;

import org.immutables.value.Value;

/**
 * Created by dumin on 2/22/17.
 */
@Value.Immutable
public interface User {

    Integer getUserId();

    String getName();

    String getAddress();

}
