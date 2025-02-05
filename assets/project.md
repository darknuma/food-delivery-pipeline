 # Project Scope and Goals 
 1. Order Fulfillment Rates
 2. Delivery Times
 3. Customer Satisfaction Scores

 **what are food delivery business data needs and pain points**

# Data Architecture 
use delta lake architecture - bronze - silver - gold, or Lambda Architecture 

## Data Ingestion:
 - Realtime messaging.
 - Message format using PARQUET
## Data processing
 - **Event Handling**: Implement logic to manage events, such as order status changes and delivery updates.

## Data storage 
   - Operational database: (cassandra)
   - data lake (S3) and data warehouse: (snowflake)
   - ~~use dbt (SQL)~~
   - Partition data based on use cases (e.g., event time).
   - Use efficient storage formats (e.g., Parquet, ORC) for batch and historical data.
   
**Should there be data cleaning and transformationm yes that is why we have the delta architecture.*
# Key Metrics
 - Order fulfillment rates
 - Delivery times
 - Order volume
 - ETA calculation for best routes
 

# How I thin about data sources:
- order placement 
- delivery progress
- driver assignment
- restaurant acceptance/rejection

# Project Phase
- Data Acquisition Layer
  - Mobile app event streams
  - GPS tracking data (estimated time of delivery)
  - Payment transaction logs (Payment Status, Payment Pending)
  - Review (Systems)

- Data Ingestion and Processing
  - Design data ingestion pipeline
  - Implemnt 
  - develop data transformation and validation
  - implement data quality  monitoring

- Data Storage and Analytics
  -  data lake to store proecess and operational data base setup
  - implement data visualization
  - develop ML models

- Observability
 - distributed tracing
 - performance scaling
 - error tracking

- Scalabilty 
 - Horizontal scalidng design
 - Auto-scaling 


# Iterative Development and Continuous Improvement:
- *Adopt an agile development approach to quickly iterate on the data engineering solution, incorporating feedback and changing business requirements.*
- *Continuously monitor and optimize the performance, scalability, and reliability of the data pipeline based on evolving usage patterns and new data sources.*
- *Leverage automated testing, continuous integration, and deployment practices to ensure the stability and maintainability of the data engineering system.*

# Testing and Validation
- **Unit Testing**: Validate individual components of your data engineering solution.
- **Integration Testing**: Ensure that all components work together seamlessly.
- **Load Testing**: Simulate high traffic to test the system’s performance under load.

# Deployment and Maintenance 

- **Deploy the Application**: Use a CI/CD pipeline for smooth deployment to production.
- **Continuous Improvement**: Regularly review system performance and gather user feedback to iterate and improve the application.


# Set up monitoring for pipelines:
- **Logging**: Use ELK Stack or Prometheus to capture errors and events in your pipelines.
- **Alerting**: Configure alerts for anomalies, such as order delays or processing lags.
- **Metrics**: Track system health (e.g., message latency, throughput).

# Develop Real-Time Use Cases
- **Dynamic Driver Matching**: Match drivers to orders based on proximity and capacity.
- **Delivery Route Optimization**: Use real-time traffic data to calculate the fastest routes.
- **Customer Notifications**: Send push notifications for order status changes.
- **Fraud Detection**: Identify anomalies in orders or driver behavior using real-time pattern analysis.


# To dos
- Generated python code for event_generator [X]
- Kafka streams produces[X]
- Consume to cassanda for operations  [X]
- Consume to S3 [x]
- Write bronze to silver layer on s3 [x]
- Write s3 layer to Snowflake [x]
- write to gold (metrics) [ ]
- write tests [ ]
- create ci/cd [ ]
- write validation tests [ ]
- write orhcestrator. (just discovered how much Snowpark is so useful) and might actually use prefect. [ ]
- write some k8s, use either kind or minikube [ ]
- write IAC with terraform [ ]
- visualize on Power BI [ ]

