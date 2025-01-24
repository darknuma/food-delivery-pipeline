import json
import boto3
from datetime import datetime
import os
from kafka import KafkaConsumer
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaS3Consumer:
    def __init__(self, bootstrap_servers: str, group_id: str, s3_bucket: str,
                 aws_region: str, aws_access_key: str, aws_secret_key: str):
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_records=100,
        )

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region,
        )
        self.bucket = s3_bucket
        self.buffer = {}

    def _get_schema(self, topic: str) -> pa.Schema:
        """Return the schema for the given topic."""
        if topic == 'food-delivery.orders.raw':
            return pa.schema([
                pa.field('event_id', pa.string()),
                pa.field('event_timestamp', pa.timestamp('ms')),
                pa.field('order_id', pa.string()),
                pa.field('merchant_id', pa.string()),
                pa.field('customer_id', pa.string()),
                pa.field('service_type', pa.string()),
                pa.field('order_status', pa.string()),
                pa.field('items', pa.list_(pa.struct([
                    pa.field('item_id', pa.string()),
                    pa.field('name', pa.string()),
                    pa.field('quantity', pa.int32()),
                    pa.field('unit_price', pa.float64()),
                    pa.field('total_price', pa.float64()),
                ]))),
                pa.field('delivery_location', pa.struct([
                    pa.field('latitude', pa.float64()),
                    pa.field('longitude', pa.float64()),
                    pa.field('address', pa.string()),
                ])),
                pa.field('delivery_fee', pa.float64()),
                pa.field('total_amount', pa.float64()),
                pa.field('estimated_delivery_time', pa.timestamp('ms')),
                pa.field('payment_method', pa.string()),
                pa.field('payment_status', pa.string()),
            ])
        elif topic == 'food-delivery.merchants.raw':
            return pa.schema([
                pa.field('event_id', pa.string()),
                pa.field('event_timestamp', pa.timestamp('ms')),
                pa.field('merchant_id', pa.string()),
                pa.field('order_id', pa.string()),
                pa.field('is_open', pa.bool_()),
                pa.field('average_preparation_time', pa.int32()),
                pa.field('current_load', pa.int32()),

            ])
        elif topic == 'food-delivery.reviews.raw':
            return pa.schema([
                pa.field('review_id', pa.string()),
                pa.field('event_id', pa.string()),
                pa.field('order_id', pa.string()),
                pa.field('customer_id', pa.string()),
                pa.field('courier_rating', pa.int32()),
                pa.field('merchant_rating', pa.int32()),
                pa.field('delivery_rating', pa.int32()),
                pa.field('feedback_timestamp', pa.timestamp('ms')),
                pa.field('comments', pa.string()),
            ])
        else:
            raise ValueError(f"Unsupported topic: {topic}")



    def _write_parquet_to_s3(self, topic: str):
        """Write buffered messages to S3 in Parquet format."""
        try:
            schema = self._get_schema(topic)
            table = pa.Table.from_pylist(self.buffer[topic], schema=schema)

            buf = BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)

            # Upload to S3
            s3_key = f"bronze/{topic.replace('.', '/')}/{datetime.now().strftime('%Y/%m/%d/%H/%M%S')}.parquet"
            self.s3_client.put_object(Bucket=self.bucket, Key=s3_key, Body=buf.getvalue())
            logger.info(f"Successfully wrote {len(self.buffer[topic])} records to s3://{self.bucket}/{s3_key}")

            # Clear the buffer for this topic
            self.buffer[topic] = []

        except Exception as e:
            logger.error(f"Error writing to S3: {str(e)}")
            raise

    def start_consuming(self, topics: list):
        """Start consuming messages from Kafka and writing to S3."""
        self.consumer.subscribe(topics)
        try:
            while True:
                messages = self.consumer.poll(timeout_ms=1000)

                for message_batch in messages.items():
                    for message in message_batch:
                        topic = message.topic
                        if topic not in self.buffer:
                            self.buffer[topic] = []

                        # Add message to buffer
                        self.buffer[topic].append(message.value)
                        logger.info(f"Consumed message from {topic}: {message.value}")

                        # Check if buffer is full
                        if len(self.buffer[topic]) >= 100:  # Adjust buffer size as needed
                            self._write_parquet_to_s3(topic)

        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
        finally:
            for topic in self.buffer:
                if self.buffer[topic]:
                    self._write_parquet_to_s3(topic)
            self.consumer.close()


if __name__ == "__main__":
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_id = os.getenv("KAFKA_GROUP_ID", "food-delivery-group")
    s3_bucket = os.getenv("S3_BUCKET", "your-s3-bucket")
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("AWS_SECRET_KEY")
    topics = os.getenv("KAFKA_TOPICS", "food-delivery.orders.raw,food-delivery.merchants.raw,food-delivery.reviews.raw").split(",")

    consumer = KafkaS3Consumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        s3_bucket=s3_bucket,
        aws_region=aws_region,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
    )
    consumer.start_consuming(topics)