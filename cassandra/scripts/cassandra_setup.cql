CREATE KEYSPACE IF NOT EXISTS food_delivery 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE food_delivery;

CREATE TABLE IF NOT EXISTS courier (
    event_id text,
    event_timestamp timestamp,
    merchant_id text,  -- fixed typo in 'merchant_id'
    courier_id text,
    order_id text,
    delivery_status text,
    delivery_location text,  -- changed from current_location to match Python code
    vehicle_type text,
    is_online boolean,
    PRIMARY KEY ((courier_id), event_timestamp)  -- added PRIMARY KEY
) WITH CLUSTERING ORDER BY (event_timestamp DESC);

CREATE TABLE IF NOT EXISTS orders (  -- changed from 'order' as it's a reserved word
    event_id text,
    event_timestamp timestamp,
    order_id text,
    merchant_id text,
    courier_id text,  -- added as it's in Python code
    customer_id text,
    service_type text,
    order_status text,
    items text,  -- specified as text since it's JSON in Python
    delivery_location text,  -- specified as text since it's JSON in Python
    delivery_fee double,
    total_amount double,
    estimated_delivery_time timestamp,
    payment_method text,
    payment_status text,
    PRIMARY KEY ((order_id), event_timestamp)
) WITH CLUSTERING ORDER BY (event_timestamp DESC);