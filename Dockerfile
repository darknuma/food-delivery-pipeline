FROM wurstmeister/kafka:latest

# Set environment variables for Kafka
ENV KAFKA_ADVERTISED_LISTENERS=INSIDE://kafka:9092,OUTSIDE://localhost:9092
ENV KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT
ENV KAFKA_LISTENERS=INSIDE://0.0.0.0:9092,OUTSIDE://0.0.0.0:9092
ENV KAFKA_ZOOKEEPER=zookeeper:2181

# Copy the topic creation script
COPY create_topics.sh /usr/local/bin/create_topics.sh
RUN chmod +x /usr/local/bin/create_topics.sh

# Command to run Kafka and create topics
CMD ["sh", "-c", "sh /usr/local/bin/create_topics.sh & /usr/local/bin/kafka-server-start.sh /opt/kafka/config/server.properties"]
