FROM --platform=$BUILDPLATFORM clojure:temurin-25-lein-alpine AS build

WORKDIR /build

COPY project.clj ./
RUN lein deps

COPY src src
COPY resources resources

RUN lein uberjar && mv target/*-standalone.jar collector.jar


FROM eclipse-temurin:25-jre-alpine

RUN apk upgrade --no-cache

WORKDIR /app

COPY --from=build /build/collector.jar collector.jar
COPY LICENSE NOTICE THIRD-PARTY-NOTICES ./

RUN mkdir -m 1777 /data && chown 10001:10001 /data

USER 10001:10001

ENV PGCUSTARD_DATA_DIR=/data

ENTRYPOINT ["java", \
  "-Xmx512m", \
  "-XX:+ExitOnOutOfMemoryError", \
  "-cp", "/app/collector.jar", \
  "clojure.main", "-m", "pgcustard.runner"]

CMD ["run"]
