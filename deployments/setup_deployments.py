# deployments/prefect/setup_deployments.py
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect.infrastructure.docker import DockerContainer
from flows.etl_flow import etl_pipeline
from flows.kafka_flow import kafka_consumer_pipeline

# Define Docker container block for Spark jobs
spark_docker_block = DockerContainer(
    image="processing:latest",  # Your Spark image
    image_pull_policy="never",  # Uses local image
    networks=["food-delivery-network"],  # Your Docker network
    volumes=["/path/to/your/data:/data"],  # Mount your data directory
    env={"PREFECT_API_URL": "http://prefect-server:4200/api"}
)

# Define Docker container for Kafka consumers
kafka_docker_block = DockerContainer(
    image="data_ingestion:latest",  # Your Kafka consumer image
    image_pull_policy="never",
    networks=["food-delivery-network"],
    env={"PREFECT_API_URL": "http://prefect-server:4200/api"}
)

# Create ETL deployment
etl_deployment = Deployment.build_from_flow(
    flow=etl_pipeline,
    name="etl-pipeline",
    schedule=CronSchedule(cron="0 2 * * *"),  # Runs daily at 2 AM
    infrastructure=spark_docker_block,
    work_queue_name="spark-queue"
)

# Create Kafka consumer deployment
kafka_deployment = Deployment.build_from_flow(
    flow=kafka_consumer_pipeline,
    name="kafka-consumer",
    schedule=CronSchedule(cron="*/5 * * * *"),  # Runs every 5 minutes
    infrastructure=kafka_docker_block,
    work_queue_name="kafka-queue"
)

if __name__ == "__main__":
    etl_deployment.apply()
    kafka_deployment.apply()