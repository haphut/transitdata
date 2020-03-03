FROM openjdk:11-jre-slim
#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl
ADD target/transitdata.jar /usr/app/transitdata.jar
ENTRYPOINT ["java", "-jar", "/usr/app/transitdata.jar"]
