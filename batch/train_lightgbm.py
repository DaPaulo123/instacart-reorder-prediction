from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("TrainLightGBM") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_catalog.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_catalog.warehouse", "hdfs://namenode:8020/iceberg_warehouse") \
    .getOrCreate()

df_final = spark.table("hadoop_catalog.instacart.final_dataset")

# Null handling
df_final = df_final.filter(col('product_id').isNotNull())
df_final = df_final.fillna(-1, subset=['favorite_department', 'favorite_aisles'])
df_final = df_final.fillna(0)

# Balance
df_positive = df_final.filter(col('label') == 1)
df_negative = df_final.filter(col('label') == 0).sample(fraction=0.065, seed=42)
df_balanced = df_positive.union(df_negative)

print(f"Balanced dataset size: {df_balanced.count()}")
df_balanced.groupBy('label').count().show()

# Convert to pandas
df_pd = df_balanced.toPandas()
print(f"Pandas shape: {df_pd.shape}")
df_pd.to_parquet('/home/jovyan/data/balanced_full.parquet')
print("Saved!")

#======================================================================#
#======================================================================#
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score, roc_auc_score
import pandas as pd
import pickle
import time

# Load saved balanced data
df_pd = pd.read_parquet('/home/jovyan/data/balanced_full.parquet')
print(f"Loaded: {df_pd.shape}")

feature_cols = ['total_orders','avg_days_between_orders','avg_basket_size','overall_reorder_rate',
               'favorite_hour','favorite_day','favorite_department','favorite_aisles',
               'product_total_orders','product_reorder_rate','product_unique_users',
               'up_times_bought','up_reorder_rate','up_avg_cart_pos','up_last_order',
               'orders_since_last_buy','up_order_rate']

X = df_pd[feature_cols]
y = df_pd['label']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

t0 = time.time()
model = lgb.LGBMClassifier(n_estimators=200, random_state=42, n_jobs=-1)
model.fit(X_train, y_train)
print(f"Training time: {time.time()-t0:.1f}s")

y_pred = model.predict(X_test)
print(f"F1: {f1_score(y_test, y_pred):.4f}")
print(f"AUC: {roc_auc_score(y_test, y_pred):.4f}")

# Save
with open('/home/jovyan/instacart-reorder-prediction/models/lgb_model.pkl', 'wb') as f:
    pickle.dump(model, f)
print("Model saved!")