CREATE KEYSPACE IF NOT EXISTS food_delivery 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE food_delivery

CREATE TABLE IF NOT EXISTS food_delivery.courier(
    event_id string,
    event_timestamp string,
    mechant_id string,
    courier_id string,
    order_id string,
    delivery_status string,
    current_location string,
    battery_level float,
    is_online boolean,
    vehicle_type string 

)

CREATE INDEX IF NOT EXISTS ON food_delivery.courier (courier_id)


CREATE TABLE IF NOT EXISTS food_delivery.order (
                event_id text,
                event_timestamp string,
                order_id test,
                merchant_id text,
                customer_id text,
                service_type string,
                order_status string,
                items ,
                delivery_location,
                delivery_fee double,
                total_amount double,
                estimated_delivery_time timestamp,
                payment_method string,
                payment_status string

            ) WITH CLUSTERING ORDER BY (timestamp DESC)


