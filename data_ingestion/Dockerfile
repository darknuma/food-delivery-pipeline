FROM python:3.11-slim

WORKDIR /app

COPY requirement.txt .
RUN pip install --no-cache-dir -r requirement.txt

COPY data_utils /app/data_utils
COPY src /app/src

CMD ["python", "./src/delivery_producer.py"]


