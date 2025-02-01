FROM bitnami/spark:3.5.3

# Install curl
USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Set environment variables for Spark and Hadoop
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Install necessary Python packages (if needed)
RUN pip install pyarrow boto3 python-dotenv 

# Add required Spark packages
ENV SPARK_JARS_PACKAGES="org.apache.hadoop:hadoop-aws:3.3.1,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3"

# Download and add the required JAR files to Spark's jars directory
RUN mkdir -p $SPARK_HOME/jars && \
    curl -o $SPARK_HOME/jars/hadoop-aws-3.3.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    curl -o $SPARK_HOME/jars/spark-snowflake_2.12-2.11.0-spark_3.3.jar https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.11.0-spark_3.3/spark-snowflake_2.12-2.11.0-spark_3.3.jar

# Copy your Spark application code
COPY . /app
WORKDIR /app