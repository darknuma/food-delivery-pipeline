# Stage 1: Data Generation
FROM python:3.9-slim AS data_generation

# Set the working directory
WORKDIR /app/data_generation

# Copy the requirements file for data generation
COPY init_setup/requirement.txt .

# Install the dependencies for data generation
RUN pip install --no-cache-dir -r requirement.txt

# Copy the data generation source code
COPY data_generation/ .