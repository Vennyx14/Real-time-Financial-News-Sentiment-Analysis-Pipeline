import streamlit as st
import pandas as pd
import pymongo
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from vnstock import Vnstock

st.set_page_config(page_title="Financial News Sentiment & Market", layout="wide", page_icon="📈")

# ==========================================
# 1. HÚT DỮ LIỆU STREAMING (REAL-TIME VIEW)
# ==========================================
@st.cache_data(ttl=10)
def load_news_data():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["financial_db"]
    collection = db["news_sentiment"]
    
    data = list(collection.find({}, {"_id": 0})) 
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date
    df = df.sort_values(by="timestamp", ascending=False)
    return df

# ==========================================
# 2. HÚT DỮ LIỆU BATCH (BATCH VIEW)
# ==========================================
@st.cache_data(ttl=60)
def load_batch_data():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["financial_db"]
    collection = db["daily_reports"]
    
    data = list(collection.find({}, {"_id": 0})) 
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    df = df.sort_values(by=["date", "source"], ascending=[False, True])
    return df

# ==========================================
# 3. GỌI API THỊ TRƯỜNG CHỨNG KHOÁN (VN-INDEX)
# ==========================================
@st.cache_data(ttl=3600)
def load_market_data():
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    try:
        market = Vnstock().stock(symbol='VNINDEX', source='VCI')
        df_index = market.quote.history(start=start_date, end=end_date)
        return df_index
    except Exception as e:
        st.error(f"Lỗi kết nối API Chứng khoán: {e}")
        return pd.DataFrame()

# ==========================================
# 4. XÂY DỰNG GIAO DIỆN (UI) LAMBDA ARCHITECTURE
# ==========================================
st.title("📊 Dashboard Phân Tích Cảm Xúc & Thị Trường (Lambda Architecture)")
st.markdown("Hệ thống xử lý Big Data kết hợp luồng Streaming (Real-time) và luồng Batch (Historical) để phân tích tương quan với VN-Index.")

df_news = load_news_data()
df_market = load_market_data()
df_batch = load_batch_data()

if df_news.empty:
    st.warning("Chưa có dữ liệu tin tức trong Database. Vui lòng chạy Producer và Spark processor!")
else:
    # --- THỐNG KÊ NHANH (METRICS) ---
    total_news = len(df_news)
    pos_news = len(df_news[df_news['sentiment_label'] == 'POS'])
    neg_news = len(df_news[df_news['sentiment_label'] == 'NEG'])
    neu_news = len(df_news[df_news['sentiment_label'] == 'NEU'])
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Tổng tin lưu trữ", total_news)
    col2.metric("Tích cực (POS) 🟢", pos_news)
    col3.metric("Tiêu cực (NEG) 🔴", neg_news)
    col4.metric("Trung lập (NEU) ⚪", neu_news)
    
    st.markdown("---")
    
    # --- TẦNG 1: SPEED LAYER (STREAMING VÀ ĐỐI CHIẾU THỊ TRƯỜNG) ---
    st.subheader("Biểu Đồ Cảm Xúc & Tương Quan Thị Trường")
    
    # HÀNG 1: Biểu đồ tròn và Biểu đồ cột nguồn báo
    row1_col1, row1_col2 = st.columns([1, 1.5])
    
    with row1_col1:
        pie_fig = px.pie(
            names=['Tích cực', 'Tiêu cực', 'Trung lập'],
            values=[pos_news, neg_news, neu_news],
            color_discrete_sequence=['#2ecc71', '#e74c3c', '#95a5a6'],
            title="Tỷ trọng Cảm xúc Tổng quan"
        )
        st.plotly_chart(pie_fig, width='stretch')
        
    with row1_col2:
        source_sentiment = df_news.groupby(['source', 'sentiment_label']).size().reset_index(name='count')
        bar_fig = px.bar(
            source_sentiment, 
            x='source', y='count', color='sentiment_label',
            color_discrete_map={'POS': '#2ecc71', 'NEG': '#e74c3c', 'NEU': '#95a5a6'},
            barmode='group',
            title="So sánh sắc thái giữa 5 nguồn báo chí"
        )
        st.plotly_chart(bar_fig, width='stretch')

    # HÀNG 2: Biểu đồ đường VN-Index trải dài toàn màn hình
    if not df_market.empty:
        fig_market = go.Figure()
        fig_market.add_trace(go.Scatter(
            x=df_market['time'], y=df_market['close'],
            mode='lines+markers', name='VN-Index',
            line=dict(color='#3498db', width=3), marker=dict(size=6)
        ))
        fig_market.update_layout(
            title="Đường đi chỉ số VN-Index (30 ngày gần nhất)",
            xaxis_title="Ngày", yaxis_title="Điểm số", template="plotly_dark",
            height=400 # Điều chỉnh chiều cao cho vừa vặn
        )
        st.plotly_chart(fig_market, width='stretch')
    else:
        st.info("Không thể tải dữ liệu VN-Index.")
            
    st.markdown("---")
    
    # --- TẦNG 2: BATCH LAYER (DỮ LIỆU ĐÃ QUA XỬ LÝ LÔ) ---
    st.subheader("Lớp Xử Lý Lô - Báo Cáo Tổng Hợp")
    st.markdown("Dữ liệu được PySpark xử lý hàng loạt vào cuối ngày, tính toán tỷ lệ cảm xúc theo từng đầu báo và lưu trữ lũy đẳng.")
    
    if not df_batch.empty:
        df_batch_display = df_batch.rename(columns={
            "date": "Ngày", "source": "Nguồn Báo", 
            "POS": "SL Tích cực", "NEG": "SL Tiêu cực", "NEU": "SL Trung lập",
            "total_news": "Tổng bài", 
            "pos_ratio_percent": "% Tích cực", "neg_ratio_percent": "% Tiêu cực"
        })
        st.dataframe(
            df_batch_display.style.format({"% Tích cực": "{:.2f}%", "% Tiêu cực": "{:.2f}%"}), 
            width='stretch'
        )
    else:
        st.info("Chưa có dữ liệu Batch. Hãy chạy file batch_processor.py để tạo báo cáo tổng hợp.")

    st.markdown("---")

    # --- TẦNG 3: LIVE FEED VĂN BẢN THÔ ---
    st.subheader("Dòng chảy Tin tức Vĩ mô")
    
    def color_sentiment(val):
        color = '#2ecc71' if val == 'POS' else '#e74c3c' if val == 'NEG' else '#95a5a6'
        return f'color: {color}; font-weight: bold;'
    
    display_df = df_news[['timestamp', 'source', 'headline', 'sentiment_label', 'confidence_score']].head(50)
    st.dataframe(display_df.style.map(color_sentiment, subset=['sentiment_label']), width='stretch')

if st.button("Làm mới dữ liệu (Refresh)"):
    st.rerun()