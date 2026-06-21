# Hệ Thống Phân Tích Cảm Xúc Tin Tức Tài Chính & Đối Chiếu Thị Trường (Lambda Architecture)

Dự án này xây dựng một Data Pipeline phân tán, ứng dụng **Kiến trúc Lambda (Lambda Architecture)** để thu thập, phân tích cảm xúc tin tức vĩ mô tiếng Việt theo thời gian thực (Real-time) và xử lý lô (Batch). Hệ thống sử dụng mô hình học sâu **PhoBERT** để đánh giá tâm lý thị trường và trực quan hóa mối tương quan với chỉ số chứng khoán **VN-Index**.

Đồ án được thực hiện trong khuôn khổ môn học *Kỹ thuật và Công nghệ Dữ liệu lớn* tại Viện Trí tuệ Nhân tạo - Trường Đại học Công nghệ (UET-VNU).

## Tính năng nổi bật
* **Thu thập Tự động (Automated Ingestion):** Quét tin tức tài chính từ 5 tòa soạn lớn (VnExpress, Dân Trí, Tuổi Trẻ, Thanh Niên, Vietnamnet) thông qua RSS Feeds.
* **AI NLP Thời gian thực:** Tích hợp mô hình `PhoBERT` (Hugging Face) vào nhân của Apache Spark để gán nhãn sắc thái (Tích cực, Tiêu cực, Trung lập) ngay khi tin tức vừa xuất bản.
* **Cơ chế Lũy đẳng (Idempotency):** Đảm bảo không trùng lặp dữ liệu 100% nhờ kết hợp RAM Cache tại Producer và Unique Index tại MongoDB.
* **Trực quan hóa Tương quan:** So sánh trực quan dòng chảy cảm xúc báo chí với biến động thực tế của VN-Index.

---

## Kiến Trúc Hệ Thống (Lambda Architecture)

Hệ thống được chia thành 4 phân hệ cốt lõi hoạt động hoàn toàn độc lập (Decoupled):

![Sơ đồ kiến trúc](architecture_diagram.png)

1. **Ingestion Layer:** Bot Python cào dữ liệu thô, tiền xử lý HTML/ISO, và đẩy vào hàng đợi của **Apache Kafka**.
2. **Speed Layer (Stream Processing):** **PySpark Structured Streaming** lắng nghe Kafka, chia micro-batch, dùng PhoBERT to inference điểm AI và lưu kết quả vào Collection `news_sentiment` của **MongoDB**.
3. **Batch Layer (Batch Processing):** PySpark khởi chạy định kỳ, gom nhóm dữ liệu lịch sử trên Datalake, tính toán tỷ trọng % cảm xúc theo ngày/nguồn báo và đẩy vào Collection `daily_reports`.
4. **Serving Layer:** Giao diện **Streamlit** đọc dữ liệu từ MongoDB kết hợp gọi API chứng khoán từ thư viện `vnstock` để kết xuất Dashboard phân tích tổng hợp.

---

## Công Nghệ Sử Dụng

* **Ngôn ngữ:** Python 3.11
* **Message Broker:** Apache Kafka (Zookeeper)
* **Big Data Processing:** Apache Spark (PySpark)
* **AI / NLP Model:** PhoBERT (Transformers, PyTorch)
* **Database / Datalake:** MongoDB
* **Giao diện (UI):** Streamlit, Plotly
* **Dữ liệu tài chính:** Vnstock API

---

## Hướng dẫn Cài đặt & Vận hành

### Bước 1: Chuẩn bị môi trường (Prerequisites)
1. Cài đặt Docker & Docker Compose (để chạy Kafka và MongoDB nếu không cài trực tiếp).
2. Đảm bảo đã thiết lập biến môi trường `JAVA_HOME` (Hỗ trợ Java 17/21).
3. Cài đặt các thư viện Python cần thiết:
   ```bash
   pip install pyspark kafka-python pymongo streamlit plotly pandas vnstock transformers torch
   ```

### Bước 2: Khởi động Hạ tầng (Infrastructure)
Bật Kafka Server và MongoDB Server (Localhost mặc định `27017`).

### Bước 3: Khởi chạy Hệ thống Data Pipeline
Để hệ thống hoạt động hoàn chỉnh, bạn cần mở **4 cửa sổ Terminal riêng biệt** và chạy lần lượt các lệnh sau:

**Terminal 1: Khởi động Producer (Ingestion Layer)**
Quét báo và đẩy dữ liệu vào Kafka (Chu kỳ 30 phút/lần):
```bash
python producer.py
```

**Terminal 2: Khởi động Spark Streaming (Speed Layer)**
Lắng nghe Kafka, xử lý AI và ghi dữ liệu Real-time xuống MongoDB:
```bash
python spark_processor.py
```

**Terminal 3: Khởi động Giao diện Web (Serving Layer)**
Mở trang Dashboard phân tích tương quan thị trường:
```bash
streamlit run dashboard.py
```

**Terminal 4: Khởi động Spark Batch (Batch Layer)**
*(Chỉ chạy vào cuối ngày hoặc khi cần tổng hợp báo cáo lịch sử)*
```bash
python batch_processor.py
```

---

## Lưu ý quan trọng
* Để tránh làm phình to Repository do các file trạng thái cục bộ của Spark sinh ra liên tục, đảm bảo file `.gitignore` của bạn đã có dòng sau: `spark_checkpoint*/`
* Nếu gặp lỗi `getSubject is supported only if a security manager is allowed` khi chạy Spark trên Java 21+, hệ thống đã tự động xử lý thông qua biến môi trường `_JAVA_OPTIONS`.

---

## Tác giả
* **Hà Đức Dũng** (24022300)
* Viện Trí tuệ Nhân tạo - Trường Đại học Công nghệ, ĐHQGHN (UET-VNU)