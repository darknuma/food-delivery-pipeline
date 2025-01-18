FROM python:3.9-slim AS data_ingestion

# Set the working directory
WORKDIR /app/data_ingestion

# Copy the requirements file for data ingestion
COPY data_ingestion/requirement.txt .

# Install the dependencies for data ingestion
RUN pip install --no-cache-dir -r requirement.txt

# Copy the data ingestion source code
COPY data_ingestion/ .

# Copy the data generation code from the previous stage
COPY --from=data_generation /app/data_generation /app/data_generation

# Command to run the Kafka producer (adjust as necessary)
CMD ["python", "data_ingestion/producers.py"]