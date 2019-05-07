FROM openjdk:8-alpine
ENV http_proxy http://webproxyjb.fpl.com:8080
ENV https_proxy http://webproxyjb.fpl.com:8080
RUN apk --update add wget tar bash
RUN wget http://apache.claz.org/spark/spark-2.3.3/spark-2.3.3-bin-hadoop2.7.tgz
RUN tar -xzf spark-2.3.3-bin-hadoop2.7.tgz && \
mv spark-2.3.3-bin-hadoop2.7 /spark && \
rm spark-2.3.3-bin-hadoop2.7.tgz
COPY start-master.sh /start-master.sh
COPY start-worker.sh /start-worker.sh
