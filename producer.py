import time
import json
import html
import feedparser
import pymongo
from kafka import KafkaProducer

RSS_FEEDS = {
    "VnExpress": "https://vnexpress.net/rss/kinh-doanh.rss",
    "DanTri": "https://dantri.com.vn/rss/kinh-doanh.rss", 
    "TuoiTre": "https://tuoitre.vn/rss/kinh-doanh.rss",   
    "Vietnamnet": "https://vietnamnet.vn/rss/kinh-doanh.rss",
    "ThanhNien": "https://thanhnien.vn/rss/kinh-te.rss"
}

MAGIC_BROWSER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36..."

def fix_vietnamese_font(text):
    text = html.unescape(text)
    try:
        return text.encode('iso-8859-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text

print("Đang kết nối với Kafka Broker...")
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# ==========================================================
# CƠ CHẾ KHỬ TRÙNG TỪ ĐẦU NGUỒN (PERSISTENT CACHE WARM-UP)
# ==========================================================
print("Đang đồng bộ danh sách link từ MongoDB để chống trùng lặp...")
try:
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
    db = mongo_client["financial_db"]
    collection = db["news_sentiment"]
    
    # Lấy duy nhất trường "link" của tất cả các bài báo cũ trong DB
    existing_links = collection.distinct("link")
    seen_links = set(existing_links)
    print(f"-> Khởi động thành công! Đã ghi nhớ {len(seen_links)} bài báo cũ từ Database.")
except Exception as e:
    print(f"[CẢNH BÁO] Không thể kết nối MongoDB để lấy cache, khởi tạo bộ nhớ rỗng: {e}")
    seen_links = set()
is_first_run = True

def fetch_and_send_news():
    global is_first_run
    new_articles_count = 0
    limit = 10 if is_first_run else 2
    
    for source_name, rss_url in RSS_FEEDS.items():
        try:
            # Gắn mặt nạ Chrome vào lệnh lấy tin để đi xuyên tường lửa
            feed = feedparser.parse(rss_url, agent=MAGIC_BROWSER_AGENT)
            
            for entry in feed.entries[:limit]:
                link = entry.link
                
                if link not in seen_links:
                    # Nắn lại font chữ tiêu đề trước khi gửi
                    clean_headline = fix_vietnamese_font(entry.title)
                    
                    data = {
                        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                        "headline": clean_headline,
                        "link": link,
                        "source": source_name
                    }
                    
                    producer.send('financial-news', value=data)
                    seen_links.add(link)
                    new_articles_count += 1
                    
                    print(f"[{source_name}] Đã gửi: {clean_headline}")
                    
        except Exception as e:
            print(f"[LỖI] Không thể lấy dữ liệu từ {source_name}: {e}")

    if is_first_run:
        print("\n[HỆ THỐNG] Đã hoàn tất mẻ crawl khởi động. Chuyển sang chế độ duy trì (2 bài/nguồn).")
        is_first_run = False

    return new_articles_count

if __name__ == "__main__":
    while True:
        print("\n--- Bắt đầu chu kỳ quét mới ---")
        count = fetch_and_send_news()
        
        if count == 0:
            print("Không có tin tức nào mới. Chờ chu kỳ 30 phút tiếp theo...")
        else:
            print(f"-> Hoàn tất gửi {count} bài báo mới vào Kafka.")
            
        # Tạm nghỉ 30 phút (1800 giây) trước khi quét lại
        time.sleep(1800)