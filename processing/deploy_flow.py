# deploy_flow.py (place in processing directory)
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect_pipeline import food_delivery_pipeline

deployment = Deployment.build_from_flow(
    flow=food_delivery_pipeline,
    name="food-delivery-daily",
    version="1.0",
    work_queue_name="food-delivery",
    schedule=CronSchedule(cron="0 2 * * *"),  # Runs daily at 2 AM
    tags=["production", "etl"]
)

if __name__ == "__main__":
    deployment.apply()