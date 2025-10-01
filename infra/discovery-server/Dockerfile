FROM amazoncorretto:21-alpine
LABEL authors="D Gorokhov"
COPY target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
