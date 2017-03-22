#!/usr/bin/env bash

mvn clean install

mvn dependency:get \
    -DrepoUrl=http://.../ \
        -Dartifact=com.myproj:spring-boot:0.5-SNAPSHOT:war \
        -Dtransitive=false \
        -Ddest=mvc-app.war

docker-compose up