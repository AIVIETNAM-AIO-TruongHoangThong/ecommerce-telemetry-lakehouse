FROM apache/spark:3.4.1-python3

USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Download JAR dependencies for Snowflake Connector and Hadoop AWS
WORKDIR /opt/spark/jars
RUN curl -O https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.4/spark-snowflake_2.12-2.12.0-spark_3.4.jar && \
    curl -O https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.33/snowflake-jdbc-3.13.33.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Install Python dependencies
RUN pip install --no-cache-dir pyspark==3.4.1 pyyaml

USER 1001
WORKDIR /opt/spark
