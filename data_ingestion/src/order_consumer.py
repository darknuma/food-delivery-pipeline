# import json
# import boto3
# from datetime import datetime
# import os
# from kafka import KafkaConsumer
# import pyarrow as pa
# import pyarrow.parquet as pq
# from io import BytesIO
# import logging
# from dotenv import load_dotenv

# load_dotenv()

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# class KafkaS3Consumer:
#     def __init__(self, bootstrap_servers: str, group_id: str, s3_bucket: str,
#                  aws_region: str, aws_access_key: str, aws_secret_key: str):
#         self.consumer = KafkaConsumer(
#             bootstrap_servers=bootstrap_servers,
#             group_id=group_id,
#             value_deserializer=lambda x: json.loads(x.decode('utf-8')),
#             auto_offset_reset='earliest',
#             enable_auto_commit=False,
#             max_poll_records=100,
#         )

#         self.s3_client = boto3.client(
#             's3',
#             aws_access_key_id=aws_access_key,
#             aws_secret_access_key=aws_secret_key,
#             region_name=aws_region,
#         )
#         self.bucket = s3_bucket
#         self.buffer = {}

#     def _get_schema(self, topic: str) -> pa.Schema:
#         """Return the schema for the given topic."""
#         if topic == 'food-delivery-orders-raw':
#             return pa.schema([
#                 pa.field('event_id', pa.string()),
#                 pa.field('event_timestamp', pa.timestamp('ms')),
#                 pa.field('order_id', pa.string()),
#                 pa.field('merchant_id', pa.string()),
#                 pa.field('customer_id', pa.string()),
#                 pa.field('service_type', pa.string()),
#                 pa.field('order_status', pa.string()),
#                 pa.field('items', pa.list_(pa.struct([
#                     pa.field('item_id', pa.string()),
#                     pa.field('name', pa.string()),
#                     pa.field('quantity', pa.int32()),
#                     pa.field('unit_price', pa.float64()),
#                     pa.field('total_price', pa.float64()),
#                 ]))),
#                 pa.field('delivery_location', pa.struct([
#                     pa.field('latitude', pa.float64()),
#                     pa.field('longitude', pa.float64()),
#                     pa.field('address', pa.string()),
#                 ])),
#                 pa.field('delivery_fee', pa.float64()),
#                 pa.field('total_amount', pa.float64()),
#                 pa.field('estimated_delivery_time', pa.timestamp('ms')),
#                 pa.field('payment_method', pa.string()),
#                 pa.field('payment_status', pa.string()),
#             ])
#         elif topic == 'food-delivery-merchants-raw':
#             return pa.schema([
#                 pa.field('event_id', pa.string()),
#                 pa.field('event_timestamp', pa.timestamp('ms')),
#                 pa.field('merchant_id', pa.string()),
#                 pa.field('order_id', pa.string()),
#                 pa.field('is_open', pa.bool_()),
#                 pa.field('average_preparation_time', pa.int64()),
#                 pa.field('current_load', pa.int64()),
#             ])
#         elif topic == 'food-delivery-reviews-raw':
#             return pa.schema([
#                 pa.field('review_id', pa.string()),
#                 pa.field('event_id', pa.string()),
#                 pa.field('order_id', pa.string()),
#                 pa.field('customer_id', pa.string()),
#                 pa.field('courier_rating', pa.int64()),
#                 pa.field('merchant_rating', pa.int64()),
#                 pa.field('delivery_rating', pa.int64()),
#                 pa.field('feedback_timestamp', pa.timestamp('ms')),
#                 pa.field('comments', pa.string()),
#             ])
#         else:
#             raise ValueError(f"Unsupported topic: {topic}")

#     def _clean_message(self, topic: str, message: dict) -> dict:
#         """Clean and validate the message to match the schema."""
#         def safe_int_convert(value, default=0):
#             """Safely convert value to integer, returning default if conversion fails."""
#             if value is None or value == "":
#                 return default
#             try:
#                 return int(value)
#             except (ValueError, TypeError):
#                 logger.error(f"Failed to convert to int: {value}")
#                 return default

#         def safe_float_convert(value, default=0.0):
#             """Safely convert value to float, returning default if conversion fails."""
#             if value is None or value == "":
#                 return default
#             try:
#                 return float(value)
#             except (ValueError, TypeError):
#                 logger.error(f"Failed to convert to float: {value}")
#                 return default

#         if topic == "food-delivery-orders-raw":
#             # Clean items array
#             if "items" in message:
#                 cleaned_items = []
#                 for item in message["items"]:
#                     if item is not None:  # Skip None items
#                         cleaned_item = {
#                             "item_id": str(item.get("item_id", "")),
#                             "name": str(item.get("name", "")),
#                             "quantity": safe_int_convert(item.get("quantity")),
#                             "unit_price": safe_float_convert(item.get("unit_price")),
#                             "total_price": safe_float_convert(item.get("total_price"))
#                         }
#                         cleaned_items.append(cleaned_item)
#                 message["items"] = cleaned_items

#         elif topic == "food-delivery-merchants-raw":
#             # Clean merchant data
#             message["average_preparation_time"] = safe_int_convert(
#                 message.get("average_preparation_time"))
#             message["current_load"] = safe_int_convert(
#                 message.get("current_load"))
#             message["is_open"] = bool(message.get("is_open", False))

#         elif topic == "food-delivery-reviews-raw":
#             # Clean review ratings
#             message["courier_rating"] = safe_int_convert(
#                 message.get("courier_rating"))
#             message["merchant_rating"] = safe_int_convert(
#                 message.get("merchant_rating"))
#             message["delivery_rating"] = safe_int_convert(
#                 message.get("delivery_rating"))

#         return message

#     def _write_parquet_to_s3(self, topic: str):
#         """Write buffered messages to S3 in Parquet format."""
#         try:
#             schema = self._get_schema(topic)
            
#             # Create a batch from the buffered data
#             batches = []
#             for message in self.buffer[topic]:
#                 # Convert cleaned message into a list of Arrow arrays
#                 fields = []
#                 for field in schema:
#                     field_name = field.name
#                     field_type = field.type
#                     value = message.get(field_name)
#                     if isinstance(value, list):
#                         # Handle list fields (e.g., items in the food-delivery-orders-raw schema)
#                         field_type = pa.list_(field_type.value_field)
#                         value = [pa.StructArray.from_pandas(value)]
#                     elif isinstance(value, dict):
#                         # Handle nested structs
#                         field_type = pa.struct([pa.field(k, v) for k, v in value.items()])
#                         value = [pa.StructArray.from_pandas(value)]
                    
#                     # Debug unexpected types
#                     elif field_type == pa.int32() or field_type == pa.int64():
#                         if not isinstance(value, int):
#                             logger.error(f"Expected int for field '{field_name}', got {type(value)}: {value}")
#                     elif field_type == pa.float64():
#                         if not isinstance(value, float):
#                             logger.error(f"Expected float for field '{field_name}', got {type(value)}: {value}")

#                     # Append to the list of field values
#                     fields.append(pa.array(value, type=field_type))
                
#                 # Add this record to the batch
#                 batch = pa.RecordBatch.from_arrays(fields, schema=schema)
#                 batches.append(batch)
            
#             # Create a table from the accumulated batches
#             table = pa.Table.from_batches(batches, schema=schema)
            
#             # Write to a buffer (in memory) and then upload to S3
#             buf = BytesIO()
#             pq.write_table(table, buf)
#             buf.seek(0)

#             # Define the S3 key
#             s3_key = f"bronze/{topic.replace('.', '/')}/{datetime.now().strftime('%Y/%m/%d/%H/%M%S')}.parquet"
#             self.s3_client.put_object(Bucket=self.bucket, Key=s3_key, Body=buf.getvalue())
#             logger.info(f"Successfully wrote {len(self.buffer[topic])} records to s3://{self.bucket}/{s3_key}")

#             # Clear the buffer for this topic
#             self.buffer[topic] = []

#         except Exception as e:
#             logger.error(f"Error writing to S3: {str(e)}")
#             raise

#     def start_consuming(self, topics: list):
#         """Start consuming messages from Kafka and writing to S3."""
#         self.consumer.subscribe(topics)
#         try:
#             while True:
#                 messages = self.consumer.poll(timeout_ms=1000)
                
#                 if not messages:
#                     continue
                
#                 # messages is a dict of TopicPartition: [MessageRecord]
#                 for topic_partition, records in messages.items():
#                     topic = topic_partition.topic
#                     if topic not in self.buffer:
#                         self.buffer[topic] = []
                    
#                     # Process each record in the batch
#                     for record in records:
#                         # Clean and validate the message
#                         cleaned_message = self._clean_message(topic, record.value)
                        
#                         # Add message to buffer
#                         self.buffer[topic].append(cleaned_message)
#                         logger.info(f"Consumed message from {topic}: {cleaned_message}")
                
#                         if len(self.buffer[topic]) >= 100:
#                             self._write_parquet_to_s3(topic)

#         except KeyboardInterrupt:
#             logger.info("Stopping consumer...")
#         finally:
#             # Flush any remaining messages in buffer
#             for topic in self.buffer:
#                 if self.buffer[topic]:
#                     self._write_parquet_to_s3(topic)
#             self.consumer.close()


# if __name__ == "__main__":
#     bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:9092")
#     group_id = os.getenv("KAFKA_GROUP_ID", "food-delivery-s3-group")
#     s3_bucket = os.getenv("S3_BUCKET", "numa-delivery")
#     aws_region = os.getenv("AWS_REGION", "us-east-2")
#     aws_access_key = os.getenv("AWS_ACCESS_KEY")
#     aws_secret_key = os.getenv("AWS_SECRET_KEY")
#     topics = os.getenv("KAFKA_TOPICS", "food-delivery-orders-raw,food-delivery-merchants-raw,food-delivery-reviews-raw").split(",")

#     consumer = KafkaS3Consumer(
#         bootstrap_servers=bootstrap_servers,
#         group_id=group_id,
#         s3_bucket=s3_bucket,
#         aws_region=aws_region,
#         aws_access_key=aws_access_key,
#         aws_secret_key=aws_secret_key,
#     )
#     consumer.start_consuming(topics)
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
