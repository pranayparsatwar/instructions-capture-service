# syntax=docker/dockerfile:1

FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw
RUN ./mvnw -q -DskipTests dependency:go-offline

COPY src/ src/
RUN ./mvnw -q -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /workspace/target/instructions-capture-service-*.jar /app/app.jar

# Set this at runtime (e.g., localstack, dockerstack, remotestack).
ENV SPRING_PROFILES_ACTIVE=dockerstack

EXPOSE 8080
ENTRYPOINT ["sh", "-c", "java -jar /app/app.jar --spring.profiles.active=${SPRING_PROFILES_ACTIVE}"]

