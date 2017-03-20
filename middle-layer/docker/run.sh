#!/usr/bin/env bash


#mvn dependency:get \
#    -DrepoUrl=http://.../ \
#        -Dartifact=org.hibernate:hibernate-entitymanager:3.4.0.GA:jar \
#        -Dtransitive=false \
#        -Ddest=component.jar \

mvn dependency:get \
    -DrepoUrl=http://.../ \
        -Dartifact=com.myproj:spring-boot:0.5-SNAPSHOT:war \
        -Dtransitive=false \
        -Ddest=mvc-app.war


docker-compose up