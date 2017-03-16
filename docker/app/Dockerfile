FROM lwieske/java-8:server-jre-8u121-slim
MAINTAINER andy.dumin@gmail.com

LABEL vendor="My"
LABEL com.example.release-date="2017-03-21"
LABEL com.example.version="0.0.5"

RUN java -version

EXPOSE 8080

#CMD ["java", "-version"]
CMD ["java", "-jar", "/app/mvc-app.war", "&>>", "/app/log/csd-console.log &"]

