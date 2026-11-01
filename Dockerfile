# syntax=docker/dockerfile:1.7
# One Dockerfile for both deployables:  docker build --build-arg MODULE=api|worker .

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY . .
ARG MODULE=api
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -pl redisflow-${MODULE} -am package -DskipTests \
 && cp redisflow-${MODULE}/target/redisflow-${MODULE}-*.jar /app.jar

FROM eclipse-temurin:21-jre
RUN groupadd --system redisflow && useradd --system --gid redisflow --home /app redisflow
WORKDIR /app
COPY --from=build /app.jar /app/app.jar
USER redisflow
# Size the heap from the container memory limit and fail fast on OOM so the orchestrator restarts us.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
