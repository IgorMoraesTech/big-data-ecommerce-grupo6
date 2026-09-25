FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

ARG FLUME_VERSION=1.11.0

USER root

RUN curl -fSL \
    https://downloads.apache.org/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz \
    -o /tmp/flume.tar.gz \
    && tar -xzf /tmp/flume.tar.gz -C /opt \
    && ln -s /opt/apache-flume-${FLUME_VERSION}-bin /opt/flume \
    && rm /tmp/flume.tar.gz

ENV FLUME_HOME=/opt/flume
ENV HADOOP_HOME=/opt/hadoop-3.2.1
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV PATH="${FLUME_HOME}/bin:${HADOOP_HOME}/bin:${PATH}"

RUN rm -f /opt/flume/lib/guava-11.0.2.jar

CMD ["flume-ng", "agent", \
     "--conf", "/opt/flume/conf", \
     "--conf-file", "/opt/project/flume/flume.conf", \
     "--name", "a1", \
     "-Dflume.root.logger=INFO,console"]
