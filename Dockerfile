# syntax=docker/dockerfile:1
# Multi-stage build. The runtime image holds a JRE and one jar: no Maven, no sources, no tests.

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY .mvn .mvn
COPY pom.xml ./
COPY broker/pom.xml broker/pom.xml
COPY client/pom.xml client/pom.xml
COPY bench/pom.xml bench/pom.xml
COPY broker/src/main broker/src/main
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp -q -pl broker package -DskipTests -Djacoco.skip=true -Dspotless.check.skip=true

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S logforge && adduser -S -G logforge -H logforge \
 && mkdir -p /data && chown logforge:logforge /data
COPY --from=build /src/broker/target/logforge-broker.jar /app/logforge-broker.jar
USER logforge
ENV LOGFORGE_DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 9092 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/health >/dev/null 2>&1 || exit 1
ENTRYPOINT ["java", "-XX:+UseG1GC", "-XX:MaxRAMPercentage=60", "-jar", "/app/logforge-broker.jar"]
