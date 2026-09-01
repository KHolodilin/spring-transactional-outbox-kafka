FROM eclipse-temurin:21-jre

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/log/app

COPY target/notification-stub-*.jar app.jar

EXPOSE 8081

HEALTHCHECK --interval=15s --timeout=5s --start-period=60s --retries=10 \
    CMD curl -sf http://localhost:8081/actuator/health | grep -q UP || exit 1

ENTRYPOINT ["java", "-jar", "app.jar"]
