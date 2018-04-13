FROM openjdk:8
EXPOSE 8080

RUN apt-get update
RUN apt-get install -y maven

WORKDIR /homework-3

ADD pom.xml /homework-3/pom.xml

RUN ["mvn", "dependency:resolve"]
RUN ["mvn", "verify"]

ADD src /homework-3/src
RUN ["mvn", "package"]

CMD ["java", "-jar", "target/cmps128-hw3-jar-with-dependencies.jar"]