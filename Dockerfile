FROM maven

# Install S6
RUN curl -sL "https://github.com/just-containers/s6-overlay/releases/download/v1.16.0.0/s6-overlay-amd64.tar.gz" | tar xz -C /

# Install Healthcheck service
RUN curl -sL -H "Accept: application/octet-stream" \
    "https://api.github.com/repos/zanox/gocheck/releases/assets/1135094?access_token=02fd63870d2264cc2f14d237385dd3dda49bd2c1" \
    > /healthcheck && chmod +x /healthcheck

WORKDIR /app
ADD / /app

RUN mvn --quiet package

EXPOSE 8080
COPY services.d /etc/services.d

CMD ["/init"]
