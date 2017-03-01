package com.my.test.go;

import org.immutables.value.Value;

/**
 * Created by dumin on 3/1/17.
 */
@Value.Immutable
public interface User {

    Integer getUserId();

    String getName();

}
