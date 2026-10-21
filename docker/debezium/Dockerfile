FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY connect-tracing/pom.xml connect-tracing/pom.xml
COPY connect-tracing/src connect-tracing/src
RUN mvn -B -f connect-tracing/pom.xml -DskipTests package

FROM quay.io/debezium/connect:3.2.4.Final
COPY --from=build /src/connect-tracing/target/connect-trace-interceptor.jar /kafka/libs/connect-trace-interceptor.jar
