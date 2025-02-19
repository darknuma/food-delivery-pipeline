# prefect_pipeline.py (place in processing directory)
from prefect import flow, task
import os
import subprocess
from datetime import datetime
from soda.scan import Scan



@task
def check_kafka_topics():
    """Verify Kafka topics exist and have data"""
    result = subprocess.run(
        ["kafka-topics", "--bootstrap-server", "kafka-broker-1:9092", "--list"],
        capture_output=True,
        text=True
    )
    topics = result.stdout.strip().split('\n')
    required_topics = [
        "food-delivery-orders-raw",
        "food-delivery-couriers-raw",
        "food-delivery-merchants-raw",
        "food-delivery-reviews-raw"
    ]
    
    missing_topics = [topic for topic in required_topics if topic not in topics]
    if missing_topics:
        raise Exception(f"Missing Kafka topics: {missing_topics}")
    
    return True

@task
def run_write_to_silver():
    """Execute the write_to_silver.py script"""
    result = subprocess.run(
        ["python", "/app/write_to_silver.py"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"write_to_silver.py failed: {result.stderr}")
    return "Silver write completed successfully"

@task
def validate_silver_data():
    scan = Scan()
    scan.add_configuration_yaml_file("/app/validation/silver_checks.yml")
    scan.set_data_source_name("spark")
    result = scan.execute()
    if result != 0:
        raise Exception("Silver data validation failed")
    return result 


@task
def run_silver_to_gold():
    """Execute the silver_to_gold.py script"""
    result = subprocess.run(
        ["python", "/app/silver_to_gold.py"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"silver_to_gold.py failed: {result.stderr}")
    return "Gold transformation completed successfully"



@task
def validate_gold_data():
    scan = Scan()
    scan.add_configuration_yaml_file("/app/validation/gold_checks.yml")
    scan.set_data_source_name("spark")
    result = scan.execute()
    if result != 0:
        raise Exception("Gold data validation failed")
    return result


@task
def run_write_to_snowflake():
    """Execute the write_to_snowflake.py script"""
    result = subprocess.run(
        ["python", "/app/write_to_snowflake.py"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"write_to_snowflake.py failed: {result.stderr}")
    return "Snowflake write completed successfully"

@task
def validate_snowflake_data():
    scan = Scan()
    scan.add_configuration_yaml_file("/app/validation/snowflake_checks.yml")
    scan.set_data_source_name("snowflake")
    result = scan.execute()
    if result != 0:
        raise Exception("Snowflake data validation failed")
    return result


@flow(name="Food Delivery ETL Pipeline")
def food_delivery_pipeline():
    """Main ETL workflow for the food delivery data pipeline"""
    # Log start time
    start_time = datetime.now()
    print(f"Pipeline started at {start_time}")
    
    # Step 1: Verify Kafka topics
    kafka_check = check_kafka_topics()
    
    # Step 2: Write data to Silver layer
    silver_result = run_write_to_silver(wait_for=[kafka_check])
    print(f"Silver layer result: {silver_result}")

    # check validation check for silver layer
    silver_validation = validate_silver_data()
    
    # Step 3: Transform Silver to Gold
    gold_result = run_silver_to_gold(wait_for=[silver_result])
    print(f"Gold layer result: {gold_result}")

    # Step 3: Transform Silver to Gold
    gold_result = run_silver_to_gold(wait_for=[silver_validation])
    
    # Step 4: Write to Snowflake
    snowflake_result = run_write_to_snowflake(wait_for=[gold_result])
    print(f"Snowflake write result: {snowflake_result}")

    # Step 6: Validate Snowflake data
    snowflake_validation = validate_snowflake_data() 
    

    # Log completion
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60
    print(f"Pipeline completed at {end_time}. Duration: {duration:.2f} minutes")
    
    return {
        "status": "success",
        "start_time": start_time,
        "end_time": end_time,
        "duration_minutes": duration
    }

# This allows running the script directly for local testing
if __name__ == "__main__":
    food_delivery_pipeline()


