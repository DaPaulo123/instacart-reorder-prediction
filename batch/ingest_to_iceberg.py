from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IngestToIceberg") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_catalog.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_catalog.warehouse", "hdfs://namenode:8020/iceberg_warehouse") \
    .getOrCreate()

# Load raw CSVs (full dataset, no sample)
df_orders = spark.read.csv("/home/jovyan/data/orders.csv", header=True, inferSchema=True)
df_prior = spark.read.csv("/home/jovyan/data/order_products__prior.csv", header=True, inferSchema=True)
df_train = spark.read.csv("/home/jovyan/data/order_products__train.csv", header=True, inferSchema=True)
df_products = spark.read.csv("/home/jovyan/data/products.csv", header=True, inferSchema=True)
df_aisles = spark.read.csv("/home/jovyan/data/aisles.csv", header=True, inferSchema=True)
df_departments = spark.read.csv("/home/jovyan/data/departments.csv", header=True, inferSchema=True)

# Fix types
from pyspark.sql.functions import col
df_products = df_products.withColumn('department_id', col('department_id').cast('integer'))
df_products = df_products.withColumn('aisle_id', col('aisle_id').cast('integer'))

# Write as Iceberg tables
df_orders.writeTo("hadoop_catalog.instacart.orders").createOrReplace()
df_prior.writeTo("hadoop_catalog.instacart.order_products_prior").createOrReplace()
df_train.writeTo("hadoop_catalog.instacart.order_products_train").createOrReplace()
df_products.writeTo("hadoop_catalog.instacart.products").createOrReplace()
df_aisles.writeTo("hadoop_catalog.instacart.aisles").createOrReplace()
df_departments.writeTo("hadoop_catalog.instacart.departments").createOrReplace()

print("All tables ingested into Iceberg!")
print(f"orders: {df_orders.count()}")
print(f"prior: {df_prior.count()}")
print(f"train: {df_train.count()}")