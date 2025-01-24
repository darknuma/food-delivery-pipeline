from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from datetime import datetime
import json
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaToCassandraConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, cassandra_hosts: list, keyspace: str):
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_records=200,  
        )

        self.cluster = Cluster(cassandra_hosts)
        self.session = self.cluster.connect(keyspace)

    def start_consuming(self, topics: list):
        """Start consuming messages from Kafka and writing to Cassandra."""
        self.consumer.subscribe(topics)
        try:
            while True:
                messages = self.consumer.poll(timeout_ms=1000)

                for message_batch in messages.items():
                    for message in message_batch:
                        self._process_record(message)

                self.consumer.commit_async()

        except Exception as e:
            logger.error(f"Error consuming messages: {str(e)}")
            raise
        finally:
            self.consumer.close()
            self.cluster.shutdown()

    def _process_record(self, record):
        """Process a Kafka message and insert it into the appropriate Cassandra table."""
        data = record.value
        try:
            if record.topic == 'food-delivery.orders.raw':
                items = json.dumps(data.get('items', [])) 
                delivery_location = json.dumps(data['delivery_location']) 
                delivery_fee = float(data['delivery_fee']) 
                total_amount = float(data['total_amount'])  

                self.session.execute("""
                    INSERT INTO active_orders (
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

            elif record.topic == 'food-delivery.couriers.raw':
                delivery_location = json.dumps(data['delivery_location'])  

                self.session.execute("""
                    INSERT INTO couriers (
                        event_id, event_timestamp, merchant_id, courier_id, order_id,
                        delivery_status, delivery_location, vehicle_type, is_online
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data['event_id'], data['event_timestamp'], data['merchant_id'],
                    data['courier_id'], data['order_id'], data['delivery_status'],
                    delivery_location, data['vehicle_type'], data['is_online']
                ))
                logger.info(f"Inserted courier {data['courier_id']} into Cassandra")

        except Exception as e:
            logger.error(f"Error processing record: {str(e)}")
            raise


if __name__ == "__main__":
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_id = os.getenv("KAFKA_GROUP_ID", "food-delivery-group")
    cassandra_hosts = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
    keyspace = os.getenv("CASSANDRA_KEYSPACE", "food_delivery")
    topics = os.getenv("KAFKA_TOPICS", "food-delivery.orders.raw,food-delivery.couriers.raw").split(",")

    consumer = KafkaToCassandraConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        cassandra_hosts=cassandra_hosts,
        keyspace=keyspace,
    )
    consumer.start_consuming(topics)