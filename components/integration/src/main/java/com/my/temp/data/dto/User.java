package com.my.temp.data.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.immutables.value.Value;

import java.math.BigDecimal;

/**
 * Created by dumin on 2/22/17.
 */
@Value.Immutable
public interface User {

    Integer getUserId();

    String getName();

    String getAddress();

}
