FROM maven

WORKDIR /app
ADD / /app

RUN mvn --quiet package

EXPOSE 8080

CMD ["/app/src/main/scripts/start.sh"]
