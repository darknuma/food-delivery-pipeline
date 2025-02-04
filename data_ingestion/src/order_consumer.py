import json
import boto3
import os
import logging
from datetime import datetime
from kafka import KafkaConsumer
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaS3Consumer:
    def __init__(self, bootstrap_servers, group_id, s3_bucket, aws_region, aws_access_key, aws_secret_key):
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
    
    def _infer_schema(self, data):
        """Dynamically infer schema from a sample of incoming messages."""
        fields = []
        for key, value in data.items():
            if isinstance(value, int):
                fields.append(pa.field(key, pa.int64()))
            elif isinstance(value, float):
                fields.append(pa.field(key, pa.float64()))
            elif isinstance(value, bool):
                fields.append(pa.field(key, pa.bool_()))
            elif isinstance(value, list):
                if all(isinstance(i, dict) for i in value):
                    struct_fields = self._infer_schema(value[0])
                    fields.append(pa.field(key, pa.list_(pa.struct(struct_fields))))
                else:
                    fields.append(pa.field(key, pa.list_(pa.string())))
            elif isinstance(value, dict):
                struct_fields = self._infer_schema(value)
                fields.append(pa.field(key, pa.struct(struct_fields)))
            else:
                fields.append(pa.field(key, pa.string()))
        return fields
    
    def _write_parquet_to_s3(self, topic):
        """Write buffered messages to S3 in Parquet format."""
        try:
            if not self.buffer[topic]:
                return
            
            schema = pa.schema(self._infer_schema(self.buffer[topic][0]))
            table = pa.Table.from_pandas(pd.DataFrame(self.buffer[topic]), schema=schema)
            
            buf = BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)
            
            s3_key = f"bronze/{topic.replace('.', '/')}/{datetime.now().strftime('%Y/%m/%d/%H/%M%S')}.parquet"
            self.s3_client.put_object(Bucket=self.bucket, Key=s3_key, Body=buf.getvalue())
            logger.info(f"Successfully wrote {len(self.buffer[topic])} records to s3://{self.bucket}/{s3_key}")
            
            self.buffer[topic] = []  # Clear buffer
        except Exception as e:
            logger.error(f"Error writing to S3: {str(e)}")
            raise
    
    def start_consuming(self, topics):
        """Start consuming messages from Kafka and writing to S3."""
        self.consumer.subscribe(topics)
        try:
            while True:
                messages = self.consumer.poll(timeout_ms=1000)
                
                if not messages:
                    continue
                
                for topic_partition, records in messages.items():
                    topic = topic_partition.topic
                    if topic not in self.buffer:
                        self.buffer[topic] = []
                    
                    for record in records:
                        self.buffer[topic].append(record.value)
                    
                    if len(self.buffer[topic]) >= 100:
                        self._write_parquet_to_s3(topic)
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
        finally:
            for topic in self.buffer:
                if self.buffer[topic]:
                    self._write_parquet_to_s3(topic)
            self.consumer.close()

if __name__ == "__main__":
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:9092")
    group_id = os.getenv("KAFKA_GROUP_ID", "food-delivery-s3-group")
    s3_bucket = os.getenv("S3_BUCKET", "numa-delivery")
    aws_region = os.getenv("AWS_REGION", "us-east-2")
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("AWS_SECRET_KEY")
    topics = os.getenv("KAFKA_TOPICS", "food-delivery-orders-raw,food-delivery-merchants-raw,food-delivery-reviews-raw").split(",")
    
    consumer = KafkaS3Consumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        s3_bucket=s3_bucket, 
        aws_region=aws_region,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
    )
    consumer.start_consuming(topics)
