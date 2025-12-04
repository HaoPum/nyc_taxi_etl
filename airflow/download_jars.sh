# Download script for Iceberg and S3 dependencies
#!/bin/bash

# Create jars directory
# Create a local jars directory inside the airflow folder
mkdir -p ./airflow/jars

# Download Iceberg Spark runtime
# wget -O /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
curl -L -o ./airflow/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar

# Download AWS SDK Bundle
# wget -O /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.367.jar \
curl -L -o ./airflow/jars/aws-java-sdk-bundle-1.12.367.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar

# Download Hadoop AWS
# wget -O /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
curl -L -o ./airflow/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# Tải Spark-Kafka connector JAR cho Spark 3.5.1 và Scala 2.12
# wget -O /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
curl -L -o ./airflow/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar

# wget -O /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
curl -L -o ./airflow/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar

# Download commons-pool2 for Kafka connector
curl -L -o ./airflow/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar


echo "JAR files downloaded successfully!"
