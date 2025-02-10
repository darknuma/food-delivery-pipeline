import pytest
import json
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from kafka import KafkaConsumer, KafkaProducer
from data_ingestion.data_utils.event_generator import DeliveryEventGenerator
from data_ingestion.src.delivery_producer import DeliveryEventProducer
import pandas as pd
import uuid

sample_data = pd.DataFrame({
    "Name": ["Restaurant A", "Restaurant B"],
    "Address": ["123 Street NY", "456 Avenue NY"]
})

def test_generate_order_event():
    generator = DeliveryEventGenerator(sample_data)
    event_id = str(uuid.uuid4())
    order_id, event = generator.generate_order_event(event_id)
    
    assert isinstance(event, dict)
    assert "order_id" in event
    assert order_id == event["order_id"]

def test_generate_merchant_event():
    generator = DeliveryEventGenerator(sample_data)
    event_id = str(uuid.uuid4())
    event = generator.generate_merchant_event(event_id)
    
    assert isinstance(event, dict)
    assert "merchant_id" in event

def test_generate_courier_event():
    generator = DeliveryEventGenerator(sample_data)
    event_id = str(uuid.uuid4())
    event = generator.generate_courier_event(event_id)
    
    assert isinstance(event, dict)
    assert "courier_id" in event

def test_generate_review_event():
    generator = DeliveryEventGenerator(sample_data)
    event_id = str(uuid.uuid4())
    order_id, order_event = generator.generate_order_event(event_id)
    generator.active_orders[order_id] = order_event
    event = generator.generate_review_event(event_id)
    
    assert isinstance(event, dict)
    assert "review_id" in event

@pytest.mark.asyncio
async def test_send_event():
    with patch("src.delivery_producer.KafkaProducer") as MockKafkaProducer:
        mock_producer = MockKafkaProducer.return_value
        mock_producer.send = MagicMock()
        producer = DeliveryEventProducer("kafka-broker-1:9092")
        producer.producer = mock_producer
        
        event = {"order_id": "123", "value": "test event"}
        await producer.send_event("orders", "123", event)
        
        mock_producer.send.assert_called()
        args, _ = mock_producer.send.call_args
        assert args[0] == "food-delivery-orders-raw"

# @pytest.mark.integration
# def test_kafka_production_and_consumption():
#     topic = "food-delivery-orders-raw"
#     bootstrap_servers = "kafka-broker-1:9092"
    
#     producer = KafkaProducer(
#         bootstrap_servers=bootstrap_servers,
#         value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#         key_serializer=str.encode
#     )
    
#     consumer = KafkaConsumer(
#         topic,
#         bootstrap_servers=bootstrap_servers,
#         auto_offset_reset='earliest',
#         enable_auto_commit=True,
#         value_deserializer=lambda x: json.loads(x.decode('utf-8'))
#     )
    
#     test_event = {"order_id": "test123", "event": "test_order"}
#     producer.send(topic, key="test123", value=test_event)
#     producer.flush()
    
#     for message in consumer:
#         received_event = message.value
#         assert received_event["order_id"] == "test123"
#         break  # Stop after first message
