from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, countDistinct, max, when, lit, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("FeatureEngineering") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_catalog.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_catalog.warehouse", "hdfs://namenode:8020/iceberg_warehouse") \
    .getOrCreate()

# Load from Iceberg
df_orders = spark.table("hadoop_catalog.instacart.orders")
df_prior = spark.table("hadoop_catalog.instacart.order_products_prior")
df_train = spark.table("hadoop_catalog.instacart.order_products_train")
df_products = spark.table("hadoop_catalog.instacart.products")

print("Loaded from Iceberg:")
print(f"orders: {df_orders.count()}, prior: {df_prior.count()}, train: {df_train.count()}")

df_orders_prior = df_orders.join(df_prior, on='order_id', how='left')

# ===== USER FEATURES =====
user_features = df_orders.groupBy('user_id').agg(
    count('order_id').alias('total_orders'),
    mean('days_since_prior_order').alias('avg_days_between_orders')
)

basket_per_order = df_orders_prior.groupBy('user_id', 'order_id').agg(
    count('product_id').alias('basket_size')
)
user_basket = basket_per_order.groupBy('user_id').agg(
    mean('basket_size').alias('avg_basket_size')
)
user_reorder = df_orders_prior.groupBy('user_id').agg(
    mean('reordered').alias('overall_reorder_rate')
)

# Favorite hour/day
window = Window.partitionBy('user_id').orderBy(col('hour_count').desc())
user_hour = df_orders.groupBy('user_id', 'order_hour_of_day').agg(count('order_id').alias('hour_count'))
user_hour = user_hour.withColumn('rank', row_number().over(window))
favorite_hour = user_hour.filter(col('rank') == 1).select('user_id', col('order_hour_of_day').alias('favorite_hour'))

window = Window.partitionBy('user_id').orderBy(col('day_count').desc())
user_dow = df_orders.groupBy('user_id', 'order_dow').agg(count('order_id').alias('day_count'))
user_day = user_dow.withColumn('rank', row_number().over(window))
favorite_day = user_day.filter(col('rank') == 1).select('user_id', col('order_dow').alias('favorite_day'))

user_features = user_features \
    .join(user_basket, on='user_id', how='left') \
    .join(user_reorder, on='user_id', how='left') \
    .join(favorite_hour, on='user_id', how='left') \
    .join(favorite_day, on='user_id', how='left')

print("User base features done. Computing department/aisle preferences...")

# Favorite department/aisle
df_orders_prior_products = df_orders_prior.join(df_products, on='product_id', how='left')

window = Window.partitionBy('user_id').orderBy(col('department_count').desc())
dept_counts = df_orders_prior_products.groupBy('user_id', 'department_id').agg(count('order_id').alias('department_count'))
dept_ranked = dept_counts.withColumn('rank', row_number().over(window))
favorite_department = dept_ranked.filter(col('rank') == 1).select('user_id', col('department_id').alias('favorite_department'))

window = Window.partitionBy('user_id').orderBy(col('aisles_count').desc())
aisle_counts = df_orders_prior_products.groupBy('user_id', 'aisle_id').agg(count('order_id').alias('aisles_count'))
aisle_ranked = aisle_counts.withColumn('rank', row_number().over(window))
favorite_aisles = aisle_ranked.filter(col('rank') == 1).select('user_id', col('aisle_id').alias('favorite_aisles'))

user_features = user_features \
    .join(favorite_department, on='user_id', how='left') \
    .join(favorite_aisles, on='user_id', how='left')

user_features.writeTo("hadoop_catalog.instacart.user_features").createOrReplace()
print(f"user_features written: {user_features.count()} rows")

# ===== PRODUCT FEATURES =====
product_features = df_orders_prior.groupBy('product_id').agg(
    count('order_id').alias('product_total_orders'),
    mean('reordered').alias('product_reorder_rate'),
    countDistinct('user_id').alias('product_unique_users')
)
product_features.writeTo("hadoop_catalog.instacart.product_features").createOrReplace()
print(f"product_features written: {product_features.count()} rows")

# ===== USER-PRODUCT FEATURES =====
up_features = df_orders_prior.groupBy('user_id', 'product_id').agg(
    count('order_id').alias('up_times_bought'),
    mean('reordered').alias('up_reorder_rate'),
    mean('add_to_cart_order').alias('up_avg_cart_pos'),
    max('order_number').alias('up_last_order')
)

user_features_small = spark.table("hadoop_catalog.instacart.user_features").select('user_id', 'total_orders')
up_features = up_features.join(user_features_small, on='user_id', how='left')
up_features = up_features.withColumn('orders_since_last_buy', col('total_orders') - col('up_last_order'))
up_features = up_features.withColumn('up_order_rate', col('up_times_bought') / col('total_orders'))
up_features = up_features.drop('total_orders')

up_features.writeTo("hadoop_catalog.instacart.up_features").createOrReplace()
print(f"up_features written: {up_features.count()} rows")

# ===== TARGET VARIABLE + FINAL JOIN =====
df_unique_prior = df_orders_prior.select('user_id', 'product_id').dropDuplicates()

df_orders_train = df_train.join(df_orders, on='order_id', how='left')
df_unique_train = df_orders_train.select('user_id', 'product_id').dropDuplicates()
df_unique_train = df_unique_train.withColumn('label', lit(1))

df_base = df_unique_prior.join(df_unique_train, on=['user_id', 'product_id'], how='left')
df_base = df_base.withColumn('label', when(col('label').isNull(), 0).otherwise(1))

user_features_full = spark.table("hadoop_catalog.instacart.user_features")
product_features_full = spark.table("hadoop_catalog.instacart.product_features")
up_features_full = spark.table("hadoop_catalog.instacart.up_features")

df_final = df_base \
    .join(user_features_full, on='user_id', how='left') \
    .join(product_features_full, on='product_id', how='left') \
    .join(up_features_full, on=['user_id', 'product_id'], how='left')

df_final.writeTo("hadoop_catalog.instacart.final_dataset").createOrReplace()
print(f"final_dataset written: {df_final.count()} rows")

print("DONE!")