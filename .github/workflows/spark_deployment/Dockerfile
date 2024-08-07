# Use a multi-arch base image
FROM --platform=$BUILDPLATFORM openjdk:11-jre-slim

# Set environment variables
ARG BUILDPLATFORM
ARG TARGETPLATFORM
ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3.3.4
ARG ICEBERG_VERSION=1.4.2
ARG AWS_SDK_VERSION=1.12.581

# Install necessary tools
RUN apt-get update && apt-get install -y curl wget procps rsync ssh

# Download and install Spark
RUN wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Download necessary JARs
RUN mkdir -p /opt/spark/jars && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar -O /opt/spark/jars/iceberg-spark-runtime.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar -O /opt/spark/jars/iceberg-aws-bundle.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -O /opt/spark/jars/hadoop-aws.jar && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -O /opt/spark/jars/aws-java-sdk-bundle.jar

# Create directory for Spark events
RUN mkdir -p /tmp/spark-events

# Create a non-root user to run Spark
RUN useradd -ms /bin/bash spark
RUN chown -R spark:spark /opt/spark /tmp/spark-events

# Switch to non-root user
USER spark
WORKDIR /opt/spark

# Expose Spark ports
EXPOSE 8080 7077 10000

CMD ["bash"]