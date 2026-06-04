FROM gradle:9.3.1-jdk21 AS builder

WORKDIR /app/consumer-balancer
COPY . .
RUN gradle test-listener:clean
RUN gradle test-listener:bootJar

FROM amazoncorretto:21.0.9

COPY --from=builder /app/consumer-balancer/test-listener/build/libs/test-listener-*.jar /app/app.jar
WORKDIR /app
CMD java -jar app.jar
