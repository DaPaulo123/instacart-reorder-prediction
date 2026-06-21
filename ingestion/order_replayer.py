from pyspark.sql.window import Window
from pyspark.sql.functions import sum, col
from pyspark.sql.functions import collect_list
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Order-Replayer") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

df_train = spark.read.csv(r'E:/Personal-Project/data/raw/order_products__train.csv', header=True, inferSchema=True)
df_orders = spark.read.csv(r'E:/Personal-Project/data/raw/orders.csv', header=True, inferSchema=True)
df_orders = df_orders.fillna(0, subset=['days_since_prior_order'])
window = Window.partitionBy('user_id').orderBy('order_number')
df_orders = df_orders.withColumn('cumulative_days', sum('days_since_prior_order').over(window))
df_orders = df_orders.withColumn('timestamp_hour', 
    col('cumulative_days') * 24 + col('order_hour_of_day'))
                                
df_joined = df_train.join(df_orders, on='order_id', how='left')
df_replay = df_joined.groupBy('order_id', 'user_id', 'order_dow', 'order_hour_of_day', 'timestamp_hour') \
    .agg(collect_list('product_id').alias('products'))


import time
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Convert to Pandas for iteration
df_replay = df_replay.orderBy('timestamp_hour').limit(1000)
df_pd = df_replay.toPandas()

for _, row in df_pd.iterrows():
    event = {
        'order_id': int(row['order_id']),
        'user_id': int(row['user_id']),
        'products': [int(p) for p in row['products']],
        'order_dow': int(row['order_dow']),
        'order_hour_of_day': int(row['order_hour_of_day']),
        'timestamp_hour': float(row['timestamp_hour'])
    }
    producer.send('orders', value=event)
    print(f"Sent order {row['order_id']} for user {row['user_id']}")
    time.sleep(0.1)

producer.flush()
print('Done replaying orders!')
user_list = df_pd['user_id'].unique().tolist()
print(f'Unique users in replay: {len(user_list)}')
print(user_list[:10])