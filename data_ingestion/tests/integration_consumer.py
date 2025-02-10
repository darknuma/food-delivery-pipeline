import pytest
import json
from unittest.mock import MagicMock, patch
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from cassandra.cluster import Cluster
from botocore.exceptions import NoCredentialsError
from datetime import datetime
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from data_ingestion.src.kafka_to_cassandra_consumer import KafkaToCassandraConsumer
from data_ingestion.src.order_consumer import KafkaS3Consumer 

@pytest.fixture
def mock_kafka_consumer():
    with patch("kafka.KafkaConsumer", autospec=True) as mock_consumer:
        yield mock_consumer

@pytest.fixture
def mock_cassandra_session():
    mock_cluster = MagicMock(spec=Cluster)
    mock_session = MagicMock()
    mock_cluster.connect.return_value = mock_session
    with patch("cassandra.cluster.Cluster", return_value=mock_cluster):
        yield mock_session

@pytest.fixture
def mock_s3_client():
    with patch("boto3.client", autospec=True) as mock_client:
        yield mock_client

@pytest.fixture
def kafka_messages():
    return [
        MagicMock(value=json.dumps({
            "event_id": "123",
            "event_timestamp": "2025-02-10T12:00:00Z",
            "order_id": "order_1",
            "merchant_id": "merchant_1",
            "courier_id": "courier_1",
            "customer_id": "customer_1",
            "service_type": "delivery",
            "order_status": "completed",
            "items": [{"item_id": "item_1", "qty": 2}],
            "delivery_location": {"lat": 40.7128, "lon": -74.0060},
            "delivery_fee": 5.99,
            "total_amount": 20.99,
            "estimated_delivery_time": "2025-02-10T12:30:00Z",
            "payment_method": "credit_card",
            "payment_status": "paid"
        }).encode("utf-8"))
    ]

@pytest.fixture
def kafka_s3_messages():
    return [
        MagicMock(value=json.dumps({"event_id": "123", "merchant_id": "merchant_1"}).encode("utf-8"))
    ]

# Test Kafka to Cassandra Consumer
def test_kafka_to_cassandra(mock_kafka_consumer, mock_cassandra_session, kafka_messages):
    mock_kafka_consumer.return_value.poll.return_value = {"food-delivery-orders-raw": kafka_messages}
    
    consumer = KafkaToCassandraConsumer("localhost:9092", "test-group", ["localhost"], "test_keyspace")
    consumer.consumer = mock_kafka_consumer.return_value
    consumer.session = mock_cassandra_session
    
    consumer.start_consuming(["food-delivery-orders-raw"])
    
    mock_cassandra_session.execute.assert_called()
    
# Test Kafka to S3 Consumer
def test_kafka_to_s3(mock_kafka_consumer, mock_s3_client, kafka_s3_messages):
    mock_kafka_consumer.return_value.poll.return_value = {"food-delivery-orders-raw": kafka_s3_messages}
    
    consumer = KafkaS3Consumer("localhost:9092", "test-group", "test-bucket", "us-east-1", "access_key", "secret_key")
    consumer.consumer = mock_kafka_consumer.return_value
    consumer.s3_client = mock_s3_client.return_value
    
    consumer.start_consuming(["food-delivery-orders-raw"])
    
    mock_s3_client.return_value.put_object.assert_called()
