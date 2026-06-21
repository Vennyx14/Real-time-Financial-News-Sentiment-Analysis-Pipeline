import os
import sys
import pymongo
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ==========================================
# CẤP PHÉP BẢO MẬT CHO JAVA 21+ (KHẮC PHỤC LỖI GETSUBJECT)
# ==========================================
os.environ["_JAVA_OPTIONS"] = "-Djava.security.manager=allow"

# Cấu hình môi trường
sys.stdout.reconfigure(encoding='utf-8')
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

print("Khởi động Lớp Xử lý Lô (Batch Layer) với PySpark...")

# ==========================================
# 1. HÚT DỮ LIỆU TỪ DATALAKE (MongoDB)
# ==========================================
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["financial_db"]
collection_raw = db["news_sentiment"]
collection_report = db["daily_reports"]

raw_data = list(collection_raw.find({}, {"_id": 0}))

if not raw_data:
    print("Kho dữ liệu trống! Cần chạy luồng Stream để tích lũy dữ liệu trước.")
    sys.exit()

# ==========================================
# 2. ĐƯA LÊN DÀN MÁY TÍNH PHÂN TÁN SPARK (CÓ GIỚI HẠN RAM)
# ==========================================
spark = SparkSession.builder \
    .appName("BatchProcessing_Lambda") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .config("spark.memory.offHeap.enabled", "false") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("WARN")

# ĐÂY CHÍNH LÀ 2 DÒNG ĐÃ TẠO RA BIẾN 'df'
pdf = pd.DataFrame(raw_data)
df = spark.createDataFrame(pdf)

print(f"Đã nạp {df.count()} bản ghi vào Spark. Bắt đầu tính toán tổng hợp...")

# ==========================================
# 3. TRANSFORMATION (Chuyển đổi & Tổng hợp dữ liệu)
# ==========================================
df = df.withColumn("date", F.to_date("timestamp"))

daily_summary = df.groupBy("date", "source") \
    .pivot("sentiment_label", ["POS", "NEG", "NEU"]) \
    .count() \
    .fillna(0)

daily_summary = daily_summary.withColumn("total_news", F.col("POS") + F.col("NEG") + F.col("NEU"))
daily_summary = daily_summary.withColumn("pos_ratio_percent", F.round((F.col("POS") / F.col("total_news")) * 100, 2))
daily_summary = daily_summary.withColumn("neg_ratio_percent", F.round((F.col("NEG") / F.col("total_news")) * 100, 2))

print("\n--- BẢNG BÁO CÁO TỔNG HỢP SAU KHI XỬ LÝ (BATCH VIEW) ---")
daily_summary.show()

# ==========================================
# 4. GHI VÀO SERVING LAYER (Lưu trữ lũy đẳng)
# ==========================================
report_records = daily_summary.toPandas().to_dict('records')

from pymongo import UpdateOne
operations = []

for row in report_records:
    date_str = row['date'].strftime('%Y-%m-%d')
    row['date'] = date_str
    
    operations.append(
        UpdateOne(
            {"date": date_str, "source": row['source']}, 
            {"$set": row}, 
            upsert=True
        )
    )

if operations:
    collection_report.bulk_write(operations)
    print(f"Đã lưu/cập nhật thành công {len(operations)} bản ghi báo cáo vào bảng 'daily_reports' trong MongoDB.")

print("Hoàn tất quy trình Batch Processing!")

spark.stop()