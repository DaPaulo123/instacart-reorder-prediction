# instacart-reorder-prediction
Personal Big Data project - Lambda architecture for predicting grocery reorders

## 1. Introduction
Predicting the customer of Instacart will reorder a product in their next order. This should help the company to track their customer's habit and arrange their stocks properly.
With over 32 million transaction records across 206,000 users, the dataset requires a Big Data pipeline to process efficiently.
---

## 2. Architecture — Lambda

```
Speed (Streaming) Layer  → Kafka + Spark Streaming + Redis
--> Real time processing

Batch Layer  → HDFS + Iceberg + Spark Batch + Airflow + MLlib
--> Historical data analysis

Serving Layer→ MongoDB + FastAPI + Grafana
--> Combining the results of Streaming layer and Batch layer

Orchestration→ Docker + Kubernetes
```

## 3. Tech Stack
```
Kafka : streams order events to the speed layer
Spark : batch + stream processing
HDFS : distributed storage for batch layer
Iceberg: table format on HDFS
Redis : prediction cache
MongoDB: serving database
Airflow : job scheduler
Grafana: dashboard
FastAPI: REST API
Docker: containerization
Kubernetes: orchestration
```
## 4. Dataset
```
DATASET: Instacart Market Basket Analysis
orders.csv              → 3.4M rows, 7 cols
order_products_prior.csv→ 32.4M rows, 4 cols
order_products_train.csv→ 1.38M rows, 4 cols
products.csv            → 49,688 rows
aisles.csv              → 134 rows
departments.csv         → 21 rows

The dataset is large enough (32M+ rows) to justify Big Data tools, and we process it through a Lambda architecture as if it were streaming data in real-time.
```
## 5. How to run
```
- Activate Docker environment:
docker-compose up

- Open Jupiter lab: http://localhost:8889/
- Take the token in the output of Docker terminal then access to the notebook
```

## 6. Project Status

✓ Data exploration complete
✓ Docker environment set up
⬜ Feature engineering
⬜ Model training
⬜ Serving layer