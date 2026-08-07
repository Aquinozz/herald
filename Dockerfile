# syntax=docker/dockerfile:1

# ---- build stage ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY . .
# Compila todos os módulos em um único reactor (common é dependência dos demais)
RUN mvn -q -DskipTests clean package

# ---- final stage ----
FROM eclipse-temurin:21-jre
ARG SERVICE=gateway
WORKDIR /app
COPY --from=build /build/${SERVICE}/target/*.jar /app/herald.jar
ENTRYPOINT ["java", "-jar", "/app/herald.jar"]