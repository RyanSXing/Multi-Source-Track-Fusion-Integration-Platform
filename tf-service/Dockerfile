FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY tf-common/pom.xml tf-common/pom.xml
COPY tf-adapters/pom.xml tf-adapters/pom.xml
COPY tf-fusion/pom.xml tf-fusion/pom.xml
COPY tf-service/pom.xml tf-service/pom.xml
RUN mvn -B -pl tf-service -am dependency:go-offline
COPY tf-common tf-common
COPY tf-adapters tf-adapters
COPY tf-fusion tf-fusion
COPY tf-service tf-service
RUN mvn -B -pl tf-service -am -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/tf-service/target/tf-service-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
