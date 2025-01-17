
from pydantic import BaseModel, Field, ConfigDict
from uuid import UUID
from enum import Enum
import random 
from datetime import datetime 
from typing import List, Optional
from decimal import Decimal 


# schemas
class ServiceType(str, Enum):
    FOOD_DELIVERY = "food_delivery"
    PACKAGE_DELIVERY = "packae_delivery"
    PHARMACY = "pharmacy"
    GROCERIES = "groceries"

class OrderStatus(str, Enum):
    CREATED = "created"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    PREPARING = "preparing"
    READY_FOR_PICKUP = "ready_for_pickup"
    PICKED_UP = "picked_up"
    EN_ROUTE = "en_route"
    ARRIVED = "arrived"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"

class DeliveryStatus(str, Enum):
    ASSIGNED = "assigned"
    ACCEPTED = "accepted"
    AT_PICKUP = "at_pickup"
    PICKED_UP = "picked_up"
    EN_ROUTE = "en_route"
    AT_DROPOFF = "at_dropoff"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"

class MerchantEvent(BaseModel):
    event_id: str
    event_timestamp: datetime
    merchant_id: str
    order_id: Optional[str]
    average_preparation_time: Optional[int] # in minutees
    is_open: bool
    current_load: int


class CustomerReview(BaseModel):
    review_id: str
    order_id: str
    customer_id: str
    courier_rating: Optional[int] = Field(None, ge=1, le=5)
    restaurant_rating: Optional[int] = Field(None, ge=1, le=5)
    delivery_rating: Optional[int] = Field(None, ge=1, le=5)
    feedback_timestamp: datetime
    comments: Optional[str]

class Location(BaseModel):
    latitude: float
    longitude: float
    address: str

class OrderItem(BaseModel):
    item_id: str
    name: str
    quantity: int
    unit_price: Decimal 
    total_price: Decimal
    # customizations: Optional[Dict[str, str]]
    # model_config = ConfigDict(arbitrary_types_allowed=True)

class PaymentMethod(str, Enum):
    CARD = "card"
    CASH = "cash"
    WALLET = "wallet"
    BANK_TRANSFER = "bank_transfer"

class PaymentStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    REFUNDED = "refunded"


class OrderEvent(BaseModel):
    event_id: str = Field(..., description="Unique identifier for the event")
    event_timestamp: datetime
    order_id: str
    merchant_id: str
    customer_id: str
    service_type: ServiceType
    order_status: OrderStatus
    items: List[OrderItem]
    delivery_location: Location
    delivery_fee: Decimal
    total_amount: Decimal
    estimated_delivery_time: datetime
    payment_method: PaymentMethod
    payment_status: PaymentStatus
    model_config = ConfigDict(arbitrary_types_allowed=True)

class CourierEvent(BaseModel):
    event_id: str
    event_timestamp: datetime
    mechant_id: str
    courier_id: str
    order_id: str
    delivery_status:DeliveryStatus
    current_location: Location
    battery_level: Optional[float]
    vehicle_type: str
    is_online: bool

    

