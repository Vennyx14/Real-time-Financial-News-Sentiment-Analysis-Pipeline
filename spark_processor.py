import os
import sys
import pymongo
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from transformers import pipeline

sys.stdout.reconfigure(encoding='utf-8')
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --driver-java-options '-Dfile.encoding=UTF-8 -Dsun.stdout.encoding=UTF-8 -Dsun.stderr.encoding=UTF-8 -Djava.security.manager=allow' pyspark-shell"

print("Đang tải mô hình PhoBERT...")

# 1. KHỞI TẠO AI OFFLINE
sentiment_analyzer = pipeline(
    "text-classification", 
    model="wonrax/phobert-base-vietnamese-sentiment",
    device=-1 
)

# 2. KẾT NỐI MONGODB 
print("Đang kết nối tới MongoDB...")
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["financial_db"]               # Tạo Database tên: financial_db
collection = db["news_sentiment"]               # Tạo Bảng chứa (Collection) tên: news_sentiment

#Tạo Unique Index cho trường link
collection.create_index("link", unique=True)

spark = SparkSession.builder.appName("FinancialNewsSentiment").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("headline", StringType(), True),
    StructField("link", StringType(), True),
    StructField("source", StringType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "financial-news") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

print("Hệ thống SS! Đang chờ dữ liệu...")

# CƠ CHẾ GOM MẺ & GHI LŨY ĐẲNG (IDEMPOTENT SINK)
def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return
        
    rows = batch_df.collect()
    headlines = [row['headline'] for row in rows]
    timestamps = [row['timestamp'] for row in rows]
    links = [row['link'] for row in rows]
    sources = [row['source'] for row in rows]
    
    try:
        results = sentiment_analyzer(headlines)
        
        from pymongo import UpdateOne
        operations = []
        
        for i in range(len(headlines)):
            record = {
                "timestamp": timestamps[i],
                "headline": headlines[i],
                "source": sources[i],
                "link": links[i],
                "sentiment_label": results[i]['label'],
                "confidence_score": float(results[i]['score'])
            }
            # Nếu tìm thấy link trùng thì KHÔNG GHI ĐÈ ($setOnInsert)
            # Nếu chưa có link này thì hành động như lệnh Insert thông thường (upsert=True)
            operations.append(
                UpdateOne({"link": links[i]}, {"$setOnInsert": record}, upsert=True)
            )
            
        if operations:
            # Ghi hàng loạt lũy đẳng xuống MongoDB
            result = collection.bulk_write(operations, ordered=False)
            print(f"--- BATCH {batch_id}: Đã đồng bộ mẻ dữ liệu. Thêm mới thực tế: {result.upserted_count} tin ---")
            
    except Exception as e:
        print(f"\n[LỖI BATCH {batch_id}]: {e}")

# 4. Kích hoạt luồng
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "./spark_checkpoint_v2") \
    .start()

query.awaitTermination()