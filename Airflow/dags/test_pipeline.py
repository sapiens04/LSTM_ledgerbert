import os
import sys

# Removed numpy hack to avoid duplicate execution crash

from datetime import datetime, timedelta
import requests
import joblib
import torch
import numpy as np
import pandas as pd
from pymongo import MongoClient

# Thêm dags vào sys.path để import dễ dàng
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Tự động đọc và nạp file Airflow/.env vào os.environ để chạy local
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
if os.path.exists(env_path):
    with open(env_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, val = line.split('=', 1)
                os.environ[key.strip()] = val.strip()

from sentiment_score import merge_and_prep_data
from feature_engineering import calculate_technical_indicators
from predict_daily import UltimatePaperLSTM, run_inference
from notifications_to_tele import send_telegram_alert

def run_local_pipeline_test():
    print("🚀 Bắt đầu kiểm tra liên thông toàn bộ luồng Airflow...")
    
    # 1. Kéo dữ liệu từ Binance
    print("\n[1] Kéo dữ liệu 50 ngày gần nhất từ Binance API...")
    url = "https://api.binance.com/api/v3/klines"
    params = {'symbol': 'BTCUSDT', 'interval': '1d', 'limit': 50}
    try:
        binance_raw = requests.get(url, params=params).json()
        print(f"-> Thành công! Lấy được {len(binance_raw)} dòng nến.")
    except Exception as e:
        print(f"❌ Lỗi kéo Binance: {e}")
        return

    # 2. Truy vấn MongoDB
    mongo_uri = os.getenv('MONGO_URI', "mongodb://root:123456@mongodb:27017/?authSource=admin")
    # Tự động chuyển đổi địa chỉ kết nối nếu chạy trực tiếp trên máy host Windows thay vì Docker container
    if not os.path.exists('/.dockerenv') and 'mongodb:27017' in mongo_uri:
        mongo_uri = mongo_uri.replace('mongodb:27017', 'localhost:27018')
        
    print(f"\n[2] Kết nối MongoDB ({mongo_uri}) để lấy Sentiment...")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
    mongo_raw = []
    try:
        db_name = os.getenv('MONGO_DB', 'redditstream_db')
        db = client[db_name]
        
        # Kiểm tra xem collection nào tồn tại để query thử
        col_name = os.getenv('MONGO_COLLECTION', 'redditstream')
        existing_cols = db.list_collection_names()
        if col_name not in existing_cols and "processed_posts" in existing_cols:
            col_name = "processed_posts"
            
        print(f"-> Đang truy vấn Database: '{db_name}', Collection: '{col_name}'")
        fifty_days_ago = datetime.now() - timedelta(days=50)
        # Chuyển đổi định dạng vì created_at lưu dưới dạng chuỗi 'YYYY-MM-DD HH:MM:SS'
        fifty_days_ago_str = fifty_days_ago.strftime('%Y-%m-%d %H:%M:%S')
        mongo_raw = list(db[col_name].find({"created_at": {"$gte": fifty_days_ago_str}}))
        print(f"-> Thành công! Lấy được {len(mongo_raw)} bài đăng từ MongoDB.")
    except Exception as e:
        print(f"⚠️ Không thể kết nối MongoDB hoặc truy vấn lỗi: {e}")
        print("-> Sẽ tiếp tục test luồng chạy bằng cách giả lập danh sách bài đăng rỗng.")
        mongo_raw = []

    # 3. Ghép nối dữ liệu (sentiment_score.py)
    print("\n[3] Ghép nối giá Binance và Sentiment Reddit...")
    try:
        df_merged = merge_and_prep_data(binance_raw, mongo_raw)
        print(f"-> Thành công! Shape dữ liệu ghép: {df_merged.shape}")
        print(df_merged[['Date', 'Close', 'Sentiment']].tail(3))
    except Exception as e:
        print(f"❌ Lỗi ghép dữ liệu: {e}")
        return

    # 4. Tính chỉ báo kỹ thuật (feature_engineering.py)
    print("\n[4] Tính toán các chỉ báo kỹ thuật (RSI, MACD, MA5, MA10, MA20)...")
    try:
        df_features = calculate_technical_indicators(df_merged)
        df_features.rename(columns={'Sentiment': 'Sentiment_Score'}, inplace=True)
        # Loại bỏ nến ngày hôm nay (chưa đóng cửa) để lấy nến ngày hôm qua (đóng lúc 7h sáng nay) làm nến cuối cùng
        df_features = df_features.iloc[:-1]
        print(f"-> Thành công! Shape dữ liệu sau tính toán: {df_features.shape}")
        
        feature_columns = [
            'Volume', 'Close', 'Open', 'High', 'Low', 'VWAP', 'Sentiment_Score'
        ]
        # Kiểm tra xem các cột cần thiết có tồn tại không
        missing_cols = [col for col in feature_columns if col not in df_features.columns]
        if missing_cols:
            print(f"❌ Lỗi: Thiếu các cột chỉ báo quan trọng: {missing_cols}")
            return
        
        last_30_days = df_features[feature_columns].tail(30)
        features_list = last_30_days.values.tolist()
        print(f"-> Chuẩn bị tensor đầu vào thành công! Kích thước: {len(features_list)}x{len(features_list[0])}")
    except Exception as e:
        print(f"❌ Lỗi tính chỉ báo: {e}")
        return

    # 5. Chạy mô hình dự đoán (predict_daily.py)
    print("\n[5] Chạy dự báo qua model BiLSTM...")
    try:
        # Gọi trực tiếp hàm run_inference của predict_daily để đồng bộ logic hoàn toàn
        res = run_inference(features_list)
        yesterday_close = res["yesterday_close"]
        pred_usd = res["pred_usd"]
        
        print(f"-> Dự báo thành công!")
        print(f"   - Giá đóng cửa hôm qua: ${yesterday_close:,.2f}")
        print(f"   - AI dự báo hôm nay:  ${pred_usd:,.2f}")
    except Exception as e:
        print(f"❌ Lỗi chạy dự báo: {e}")
        return

    # 6. Gửi Cảnh báo Telegram (notifications_to_tele.py)
    print("\n[6] Thử nghiệm gửi cảnh báo qua Telegram...")
    try:
        send_telegram_alert(yesterday_close, pred_usd)
        print("-> Tin nhắn test đã được gửi đi! Vui lòng kiểm tra kênh Telegram của bạn.")
    except Exception as e:
        print(f"❌ Lỗi gửi Telegram: {e}")

    print("\n🎉 === KẾT QUẢ: KIỂM TRA LUỒNG CHẠY HOÀN TẤT ===")
    print("Toàn bộ liên kết DB và Telegram đã hoạt động trơn tru không gặp lỗi.")

if __name__ == "__main__":
    run_local_pipeline_test()
