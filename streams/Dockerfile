FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre
USER 1000
WORKDIR /app
COPY --from=build /workspace/target/ts-streams-transform-0.1.0-shaded.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
