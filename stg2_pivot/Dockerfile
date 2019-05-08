# Based on Dockerfile: https://github.com/spikerlabs/scala-sbt (based on https://github.com/hseeberger/scala-sbt)
# Based on Tutorial at: https://towardsdatascience.com/a-journey-into-big-data-with-apache-spark-part-2-4511aa19a900

FROM openjdk:8-alpine

# Args for package versions
ARG SCALA_VERSION
ARG SBT_VERSION

# Local proxies (needed to connect to Internet to download packages behind corp ip)
ENV http_proxy http://webproxyjb.fpl.com:8080
ENV https_proxy http://webproxyjb.fpl.com:8080

# Env Vars to associate args:
ENV SCALA_VERSION ${SCALA_VERSION:-2.12.8}
ENV SBT_VERSION ${SBT_VERSION:-1.2.7}


# scala download and config:
RUN \
  echo "$SCALA_VERSION $SBT_VERSION" && \
  mkdir -p /usr/lib/jvm/java-1.8-openjdk/jre && \
  touch /usr/lib/jvm/java-1.8-openjdk/jre/release && \
  # apk: Alpine link package management command 
  # add bash and curl to installed software
  apk add --no-cache bash && \
  apk add --no-cache curl && \
  apk add --no-cache vim && \
  # Download scala package and untar to usr/local
  curl -fsL http://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz | tar xfz - -C /usr/local && \
  # Create a symbolic link for scala
  ln -s /usr/local/scala-$SCALA_VERSION/bin/* /usr/local/bin/ && \
  scala -version && \
  scalac -version

# sbt download and config:
RUN \
  curl -fsL https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.tgz | tar xfz - -C /usr/local && \
  $(mv /usr/local/sbt-launcher-packaging-$SBT_VERSION /usr/local/sbt || true) \
  ln -s /usr/local/sbt/bin/* /usr/local/bin/ && \
  sbt sbt-version || sbt sbtVersion || true

WORKDIR /project
ADD ./ ./

CMD "/usr/local/bin/sbt"
