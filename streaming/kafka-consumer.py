from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import *
import pickle
import numpy as np
from pymongo import MongoClient
import redis, json
import os, time
spark = SparkSession.builder \
    .appName("InstacartStreaming") \
    .config("spark.driver.memory", "4g") \
    .config("spark.local.dir", r"D:\spark-temp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

with open(r'D:\Personal_Project\instacart-reorder-prediction\models\lgb_model.pkl', 'rb') as f:
    model = pickle.load(f)
print('LightGBM model loaded!')


kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 200) \
    .load()


schema = StructType([
    StructField("order_id", LongType()),
    StructField("user_id", LongType()),
    StructField("products", ArrayType(LongType())),
    StructField("order_dow", IntegerType()),
    StructField("order_hour_of_day", IntegerType()),
    StructField("timestamp_hour", DoubleType())
])

parsed = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")


exploded = parsed.select(
    col("user_id"),
    explode(col("products")).alias("product_id")
)




def predict_batch(feature_vectors: list) -> list:
    feature_cols = ['total_orders','avg_days_between_orders','avg_basket_size','overall_reorder_rate',
                   'favorite_hour','favorite_day','favorite_department','favorite_aisles',
                   'product_total_orders','product_reorder_rate','product_unique_users',
                   'up_times_bought','up_reorder_rate','up_avg_cart_pos','up_last_order',
                   'orders_since_last_buy','up_order_rate']
    import pandas as pd
    X = pd.DataFrame(feature_vectors, columns=feature_cols)
    probs = model.predict_proba(X)[:, 1]
    return probs.tolist()

mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['instacart']

print("Loading features into memory cache...")
user_cache = {doc['user_id']: doc for doc in db['user_features'].find({}, {'_id': 0})}
product_cache = {doc['product_id']: doc for doc in db['product_features'].find({}, {'_id': 0})}
print(f"Cached {len(user_cache)} users, {len(product_cache)} products")

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)


def process_batch(batch_df, batch_id):
    try:
        print(f"[Batch {batch_id}] Starting, row count: {batch_df.count()}")
        rows = batch_df.collect()
        print(f"[Batch {batch_id}] Collected {len(rows)} rows")
        results_by_user = {}

        # Group rows by user
        users_products = {}
        for row in rows:
            users_products.setdefault(row['user_id'], []).append(row['product_id'])

        for user_id, product_ids in users_products.items():

            t0 = time.time()
            user_feat = user_cache.get(user_id, {})
            prod_feats = {pid: product_cache.get(pid, {}) for pid in product_ids}
            
            t1 = time.time()
            up_feats = {p['product_id']: p for p in db['up_features'].find(
                {'user_id': user_id, 'product_id': {'$in': product_ids}}, {'_id': 0})}
            t2 = time.time()
            # Build ALL feature vectors for this user at once
            feature_vectors = []
            product_keys = []

            for product_id in product_ids:
                prod_feat = prod_feats.get(product_id, {})
                up_feat = up_feats.get(product_id, {})
            
                try:
                    
                    feature_vector = [
                        float(user_feat.get('total_orders', 0)),
                        float(user_feat.get('avg_days_between_orders', 0)),
                        float(user_feat.get('avg_basket_size', 0)),
                        float(user_feat.get('overall_reorder_rate', 0)),
                        float(user_feat.get('favorite_hour', 0)),
                        float(user_feat.get('favorite_day', 0)),
                        float(user_feat.get('favorite_department', -1)),
                        float(user_feat.get('favorite_aisles', -1)),
                        float(prod_feat.get('product_total_orders', 0)),
                        float(prod_feat.get('product_reorder_rate', 0)),
                        float(prod_feat.get('product_unique_users', 0)),
                        float(up_feat.get('up_times_bought', 0)),
                        float(up_feat.get('up_reorder_rate', 0)),
                        float(up_feat.get('up_avg_cart_pos', 0)),
                        float(up_feat.get('up_last_order', 0)),
                        float(up_feat.get('orders_since_last_buy', 0)),
                        float(up_feat.get('up_order_rate', 0)),
                    ]
                    feature_vectors.append(feature_vector)
                    product_keys.append(product_id)
                except Exception as e:
                    print(f"[WARN] Skip user={user_id} product={product_id}: {e}")

            # ONE batch prediction call for all products of this user
            if feature_vectors:
                
                probs = predict_batch(feature_vectors)
                t3 = time.time()
                print(f"User {user_id} ({len(product_ids)} products): cache={t1-t0:.2f}s, mongo={t2-t1:.2f}s, predict={t3-t2:.2f}s")
                for product_id, prob in zip(product_keys, probs):
                    results_by_user.setdefault(int(user_id), []).append({
                        'product_id': int(product_id),
                        'reorder_probability': float(prob)
                    })

        for uid, preds in results_by_user.items():
            redis_client.setex(f'prediction:{uid}', 3600, json.dumps(preds))

        print(f"[Batch {batch_id}] Done: {sum(len(v) for v in results_by_user.values())} predictions")
    
    except Exception as e:
        print(f"[Batch {batch_id}] FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()

checkpoint_dir = r"D:\Personal_Project\instacart-reorder-prediction\streaming\checkpoint\instacart"
os.makedirs(checkpoint_dir, exist_ok=True)

# Convert backslash → forward slash cho Hadoop URI
checkpoint_uri = checkpoint_dir.replace("\\", "/")

query = exploded.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpoint_uri) \
    .trigger(processingTime="7 seconds") \
    .start()

query.awaitTermination()