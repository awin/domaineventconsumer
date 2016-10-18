FROM maven

WORKDIR /app
ADD / /app

RUN mkdir -p -m 0777 /srv/log/kafkarest
RUN cp /app/src/main/scripts/start.sh /app

RUN mvn --quiet package

EXPOSE 8080

CMD ["/app/start.sh"]
