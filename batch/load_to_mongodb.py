from pyspark.sql import SparkSession
from pymongo import MongoClient
import pyarrow.parquet as pq
import pandas as pd

spark = SparkSession.builder \
    .appName("LoadToMongoDB") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_catalog.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_catalog.warehouse", "hdfs://namenode:8020/iceberg_warehouse") \
    .getOrCreate()

client = MongoClient('mongodb://instacart-mongodb:27017/')
db = client['instacart']

# user_features - small enough for toPandas
print("Loading user_features...")
user_pd = spark.table("hadoop_catalog.instacart.user_features").toPandas()
db['user_features'].drop()
db['user_features'].insert_many(user_pd.to_dict('records'))
print(f"Inserted {len(user_pd)} user features")

# product_features - small enough for toPandas
print("Loading product_features...")
prod_pd = spark.table("hadoop_catalog.instacart.product_features").toPandas()
db['product_features'].drop()
db['product_features'].insert_many(prod_pd.to_dict('records'))
print(f"Inserted {len(prod_pd)} product features")

# up_features - too large, write to parquet first then chunk insert
print("Writing up_features to parquet...")
spark.table("hadoop_catalog.instacart.up_features") \
    .write.parquet('file:///home/jovyan/data/up_features_full', mode='overwrite')
print("Written. Now inserting to MongoDB in chunks...")

db['up_features'].drop()
table = pq.read_table('/home/jovyan/data/up_features_full')
total = len(table)
batch_size = 500000
print(f"Total rows: {total}")

for i in range(0, total, batch_size):
    chunk = table.slice(i, batch_size).to_pandas()
    db['up_features'].insert_many(chunk.to_dict('records'))
    print(f"Inserted rows {i} to {min(i+batch_size, total)}")

# Recreate indexes
print("Creating indexes...")
db['user_features'].create_index('user_id', unique=True)
db['product_features'].create_index('product_id', unique=True)
db['up_features'].create_index([('user_id', 1), ('product_id', 1)], unique=True)
print("Indexes created!")
print("DONE!")