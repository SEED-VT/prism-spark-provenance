# Prism on Apache Spark 4.1.2 with JupyterLab. The first stage builds the Prism jar from
# this repository, so the image always matches the source. The second stage runs Spark
# from the pyspark package, which carries the Spark jars. The jar does not depend on the
# CPU, so the first stage runs on the build machine's platform when building for others.

FROM --platform=$BUILDPLATFORM eclipse-temurin:17-jdk-jammy AS build
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
RUN curl -sL https://github.com/sbt/sbt/releases/download/v1.10.7/sbt-1.10.7.tgz | tar xz
COPY build.sbt ./
COPY project/build.properties project/
RUN ./sbt/bin/sbt -batch update
COPY src src
RUN ./sbt/bin/sbt -batch package \
    && mkdir out \
    && cp target/scala-2.13/prism_2.13-*.jar out/prism.jar \
    && cp "$(find /root/.cache/coursier -name 'fastutil-8.5.15.jar' | head -1)" out/fastutil.jar

FROM eclipse-temurin:17-jre-jammy
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip procps tini \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install --no-cache-dir pyspark==4.1.2 jupyterlab==4.2.5 nbconvert==7.16.4 \
        numpy==1.26.4 pandas==2.1.4

ENV PRISM_HOME=/opt/prism \
    SPARK_CONF_DIR=/opt/prism/conf \
    PYTHONPATH=/opt/prism/python:/opt/prism/benchmarks/disc \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3
COPY --from=build /build/out /opt/prism/jars
COPY python /opt/prism/python
COPY benchmarks/disc /opt/prism/benchmarks/disc
COPY conf/java-opens.txt /opt/prism/conf/
COPY demo/spark-defaults.conf demo/entrypoint.sh /opt/prism/conf/
RUN opens="$(tr '\n' ' ' < /opt/prism/conf/java-opens.txt)" \
    && sed -i "s#@OPENS@#$opens#" /opt/prism/conf/spark-defaults.conf \
    && chmod +x /opt/prism/conf/entrypoint.sh
COPY demo/notebooks /work/notebooks

WORKDIR /work
EXPOSE 8888 8080
ENTRYPOINT ["tini", "--", "/opt/prism/conf/entrypoint.sh"]
CMD ["lab"]
