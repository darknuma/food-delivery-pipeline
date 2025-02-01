FROM python:3.11-slim

WORKDIR /app

COPY requirement.txt .
RUN pip install --no-cache-dir -r requirement.txt

# Copy the application code
COPY data_utils /app/data_utils
COPY src /app/src

# Set the entrypoint
CMD ["python", "./src/delivery_producer.py"]


