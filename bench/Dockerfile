# syntax=docker/dockerfile:1
# Image for the benchmark and chaos client. Build from the repository root:
#   docker build -f bench/Dockerfile -t logforge-bench .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY .mvn .mvn
COPY pom.xml ./
COPY broker/pom.xml broker/pom.xml
COPY client/pom.xml client/pom.xml
COPY bench/pom.xml bench/pom.xml
COPY broker/src/main broker/src/main
COPY client/src/main client/src/main
COPY bench/src/main bench/src/main
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp -q -pl bench -am package -DskipTests -Djacoco.skip=true -Dspotless.check.skip=true

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S bench && adduser -S -G bench -H bench && mkdir -p /results /work && chown bench:bench /results /work
COPY --from=build /src/bench/target/logforge-bench-*.jar /app/
COPY --from=build /src/bench/target/lib/ /app/lib/
USER bench
ENTRYPOINT ["java", "-Xmx2g", "-cp", "/app/*:/app/lib/*", "dev.logforge.bench.Main"]
