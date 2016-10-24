FROM maven

WORKDIR /app
ADD / /app

RUN mvn --quiet package
