FROM openjdk:8
EXPOSE 8080

RUN apt-get update
RUN apt-get install -y maven

WORKDIR /homework-2

ADD pom.xml /homework-2/pom.xml

RUN ["mvn", "dependency:resolve"]
RUN ["mvn", "verify"]

ADD src /homework-2/src
RUN ["mvn", "package"]

CMD ["java", "-jar", "target/cmps128-hw2-jar-with-dependencies.jar"]