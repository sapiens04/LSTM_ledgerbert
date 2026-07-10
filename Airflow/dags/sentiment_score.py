import pandas as pd
import numpy as np

def merge_and_prep_data(binance_raw_data, mongo_raw_data):
    # 1. Xử lý dữ liệu Binance
    df_binance = pd.DataFrame(binance_raw_data)
    if df_binance.empty:
        raise ValueError("Dữ liệu Binance rỗng.")
    
    if df_binance.shape[1] >= 6:
        df_binance = df_binance.iloc[:, :6]
        df_binance.columns = ['open_time', 'Open', 'High', 'Low', 'Close', 'Volume']
    else:
        raise ValueError(f"Cấu trúc dữ liệu Binance không hợp lệ (số cột: {df_binance.shape[1]} < 6).")
        
    df_binance['Date'] = pd.to_datetime(df_binance['open_time'], unit='ms').dt.normalize()
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        df_binance[col] = df_binance[col].astype(float)

    # 2. Xử lý dữ liệu MongoDB với logic người dùng yêu cầu
    if not mongo_raw_data:
        # Xử lý ngoại lệ: Nếu Mongo trống, tạo cột Sentiment Neutral (0.0)
        df_binance['Sentiment'] = 0.0
        return df_binance

    df_mongo = pd.DataFrame(mongo_raw_data)
   
    # 2.1. Số hóa Sentiment (bullish=1, neutral=0, bearish=-1)
    sentiment_map = {'bullish': 1, 'neutral': 0, 'bearish': -1}
    # Map chữ thành số, nếu gặp chữ lạ thì cho là 0
    mapped_score = df_mongo['sentiment'].map(sentiment_map).fillna(0)
   
    # 2.2. Nhân với độ tự tin để ra điểm chất lượng thực tế
    df_mongo['final_score'] = mapped_score * df_mongo['sentiment_confidence']
   
    # 2.3. Cắt bỏ giờ/phút, chỉ lấy ngày
    df_mongo['Date'] = pd.to_datetime(df_mongo['created_at']).dt.normalize()
   
    # 2.4. GOM NHÓM THEO NGÀY (Lấy trung bình cộng điểm Sentiment)
    daily_sentiment = df_mongo.groupby('Date')['final_score'].mean().reset_index()
    daily_sentiment.rename(columns={'final_score': 'Daily_Sentiment'}, inplace=True)

    # ==========================================
    # BƯỚC 3: GHÉP NỐI VÀ LÙI NGÀY (SHIFT)
    # ==========================================
    # Ghép bảng giá và bảng Sentiment dựa trên chung cột 'Date'
    df_merged = pd.merge(df_binance, daily_sentiment, on='Date', how='left')
   
    # Điền giá trị 0 (Neutral) cho những ngày thị trường nghỉ/không có ai đăng bài Reddit
    df_merged['Daily_Sentiment'] = df_merged['Daily_Sentiment'].fillna(0.0)
   
    # LƯU Ý CHÍ MẠNG: Đẩy cột Sentiment xuống 1 dòng để model không "nhìn trộm" tương lai
    df_merged['Sentiment'] = df_merged['Daily_Sentiment'].shift(1)
   
    # Dòng đầu tiên bị shift xuống sẽ dính NaN, ta điền 0 (Neutral)
    df_merged['Sentiment'] = df_merged['Sentiment'].fillna(0.0)
   
    # Dọn dẹp cột nháp
    df_merged.drop(columns=['Daily_Sentiment'], inplace=True)
   
    return df_merged
