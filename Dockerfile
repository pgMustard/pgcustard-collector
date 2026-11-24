FROM eclipse-temurin:25-jre-alpine@sha256:69c2a26e9894016e274c880fa542956d005b3755f84db765145185e87405bd89 AS java


FROM --platform=$BUILDPLATFORM clojure:temurin-25-lein-alpine@sha256:53e907b840d5a9603aad67a0f51df42659d23359a5ab5e20ffe2ae3c4fa4c6d6 AS build

WORKDIR /build

COPY project.clj ./
RUN lein deps

COPY src src
COPY resources resources

RUN lein uberjar \
 && mkdir unpacked \
 && cd unpacked \
 && jar --extract --file ../target/*-standalone.jar \
 && sed -i -E '/^(Built-By|Build-Jdk|Created-By):/d' META-INF/MANIFEST.MF \
 # Clojure only loads a class that's newer than its source
 && find . -type f ! -name '*.class' -exec touch -d '2000-01-01 00:00:00' {} + \
 && find . -type f -name '*.class' -exec touch -d '2000-01-01 00:00:10' {} + \
 && find . -type f | LC_ALL=C sort | sed 's|^\./||' > ../files \
 && jar --create --no-manifest --file ../collector.jar @../files

RUN --mount=from=java,target=/java \
    mkdir -p /rootfs/lib/apk/db /rootfs/etc /rootfs/opt/java \
 && cp -a /java/lib/ld-musl-*.so.1 /java/lib/libc.musl-*.so.1 /rootfs/lib/ \
 && cp -a /java/opt/java/openjdk /rootfs/opt/java/ \
 && cp -L /java/etc/alpine-release /java/etc/os-release /rootfs/etc/ \
 && awk -v RS= -v ORS='\n\n' '/(^|\n)P:musl\n/' /java/lib/apk/db/installed > /rootfs/lib/apk/db/installed \
 && mkdir -m 1777 /rootfs/data /rootfs/tmp


FROM scratch

COPY --from=build /rootfs /

WORKDIR /app

COPY --from=build /build/collector.jar collector.jar
# Otherwise permissions depend on who checked the files out
COPY --chmod=644 LICENSE NOTICE THIRD-PARTY-NOTICES ./

USER 10001:10001

ENV PATH=/opt/java/openjdk/bin LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 PGCUSTARD_DATA_DIR=/data

ENTRYPOINT ["java", \
  "-Xmx512m", \
  "-XX:+ExitOnOutOfMemoryError", \
  "-cp", "/app/collector.jar", \
  "clojure.main", "-m", "pgcustard.runner"]

CMD ["run"]
