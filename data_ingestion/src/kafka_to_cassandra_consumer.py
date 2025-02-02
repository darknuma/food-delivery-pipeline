from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from datetime import datetime
import json
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaToCassandraConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, cassandra_hosts: list, keyspace: str):
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            group_id=group_id,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_records=200,
        )

        self.cluster = Cluster(cassandra_hosts, protocol_version=5)
        self.session = self.cluster.connect(keyspace)

    def start_consuming(self, topics: list):
        """Start consuming messages from Kafka and writing to Cassandra."""
        # Subscribe to the topics
        self.consumer.subscribe(topics)
        
        try:
            while True:
                # Poll for messages
                records = self.consumer.poll(timeout_ms=1000)
                
                if not records:
                    continue
                    
                for topic_partition, messages in records.items():
                    logger.info(f"Processing {len(messages)} messages from {topic_partition.topic}")
                    for record in messages:
                        try:
                            self._process_record(record)
                        except Exception as e:
                            logger.error(f"Error processing record: {str(e)}")
                            continue
                
                # Commit offsets after processing the batch
                try:
                    self.consumer.commit()
                except Exception as e:
                    logger.error(f"Error committing offsets: {str(e)}")

        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise
        finally:
            self._cleanup()

    def _cleanup(self):
        """Clean up resources."""
        try:
            self.consumer.close()
            self.cluster.shutdown()
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

    def _process_record(self, record):
        """Process a Kafka message and insert it into the appropriate Cassandra table."""
        if not hasattr(record, 'value'):
            logger.error("Record has no value attribute")
            return
            
        data = record.value
        try:
            if record.topic == 'food-delivery-orders-raw':
                items = json.dumps(data.get('items', [])) 
                delivery_location = json.dumps(data['delivery_location'])
                delivery_fee = float(data['delivery_fee']) 
                total_amount = float(data['total_amount'])  

                self.session.execute("""
                    INSERT INTO orders (
                        event_id, event_timestamp, order_id, merchant_id, courier_id,
                        customer_id, service_type, order_status, items, delivery_location,
                        delivery_fee, total_amount, estimated_delivery_time,
                        payment_method, payment_status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data['event_id'], data['event_timestamp'], data['order_id'],
                    data['merchant_id'], data.get('courier_id'), data['customer_id'],
                    data['service_type'], data['order_status'], items, delivery_location,
                    delivery_fee, total_amount, data['estimated_delivery_time'],
                    data['payment_method'], data['payment_status']
                ))
                logger.info(f"Inserted order {data['order_id']} into Cassandra")

            elif record.topic == 'food-delivery-couriers-raw':
                items = json.dumps(data.get('items', []))
                delivery_location = json.dumps(data['current_location'])
                event_timestamp = datetime.fromisoformat(data['event_timestamp'].replace('Z', '+00:00'))



                self.session.execute("""
                    INSERT INTO courier (
                        event_id, event_timestamp, merchant_id, courier_id, order_id,
                        delivery_status, current_location, vehicle_type, is_online
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data['event_id'], event_timestamp, data['merchant_id'],
                    data['courier_id'], data['order_id'], data['delivery_status'],
                    delivery_location, data['vehicle_type'], data['is_online']
                ))
                logger.info(f"Inserted courier {data['courier_id']} into Cassandra")

        except Exception as e:
            logger.error(f"Error processing record {record.topic}: {str(e)}")
            raise

if __name__ == "__main__":
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:9092")
    group_id = os.getenv("KAFKA_GROUP_ID", "food-delivery-group")
    cassandra_hosts = os.getenv("CASSANDRA_HOSTS", "cassandra").split(",")
    keyspace = os.getenv("CASSANDRA_KEYSPACE", "food_delivery")
    topics = os.getenv("KAFKA_TOPICS", "food-delivery-orders-raw,food-delivery-couriers-raw").split(",")

    consumer = KafkaToCassandraConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        cassandra_hosts=cassandra_hosts,
        keyspace=keyspace,
    )
    consumer.start_consuming(topics)