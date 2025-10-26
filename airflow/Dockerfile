FROM apache/airflow:2.7.1

USER root

# Copy files trước (ít thay đổi)
COPY requirements.txt /requirements.txt

# Cài đặt packages (thay đổi ít)
RUN apt-get update && \
    apt-get install -y wget openjdk-11-jdk curl procps && \
    apt-get clean

# Tải Spark trực tiếp từ Apache
RUN wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -O /tmp/spark-3.5.1-bin-hadoop3.tgz

# Set environment variables (thay đổi ít)
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64
ENV PATH $PATH:$JAVA_HOME/bin
ENV SPARK_HOME /opt/bitnami/spark
ENV PATH $PATH:$SPARK_HOME/bin

# Cài đặt Spark (thay đổi ít)
RUN mkdir -p /opt/bitnami && \
    tar -xzf /tmp/spark-3.5.1-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-3.5.1-bin-hadoop3 /opt/bitnami/spark && \
    rm /tmp/spark-3.5.1-bin-hadoop3.tgz

# Cài Python packages (thay đổi nhiều nhất - để cuối)
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

USER root
RUN mkdir -p /opt/spark/jobs /opt/data && \
    chown -R airflow:root /opt/spark /opt/data /opt/bitnami/spark

USER airflow





