import os
import sys

# Numpy hack removed to prevent silent interpreter crashes

import requests
import numpy as np
import pandas as pd
import joblib
import torch
import torch.nn as nn
from datetime import datetime, timedelta
from pymongo import MongoClient

# Import từ thư mục dags
from sentiment_score import merge_and_prep_data
from feature_engineering import calculate_technical_indicators

# Class Mô hình
# Class Mô hình (Đã cấu trúc lại để khớp với file weights thực tế: LSTM 2 lớp, hidden_size=50, không có Attention)
class UltimatePaperLSTM(nn.Module):
    def __init__(self, input_size=7, output_size=1, dropout_rate=0.2):
        super(UltimatePaperLSTM, self).__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=128,
            num_layers=2,
            batch_first=True
        )
        self.dropout = nn.Dropout(dropout_rate)
        self.fc = nn.Linear(128, output_size)

    def forward(self, x):
        # x shape: [batch_size, seq_len, input_size]
        out, _ = self.lstm(x)
        # Lấy output của timestep cuối cùng (seq_len - 1) để đưa vào fully connected layer
        out = out[:, -1, :]
        return self.fc(self.dropout(out))

def fetch_and_prepare_features():
    """Kéo dữ liệu và xử lý thành mảng 10 ngày"""
    url = "https://api.binance.com/api/v3/klines"
    params = {'symbol': 'BTCUSDT', 'interval': '1d', 'limit': 50}
    binance_raw = requests.get(url, params=params).json()
    
    mongo_uri = os.getenv('MONGO_URI')
    db_name = os.getenv('MONGO_DB')
    client = MongoClient(mongo_uri)
    db = client[db_name]
    
    # Tự động nhận diện collection hiện có (redditstream hoặc processed_posts)
    col_name = os.getenv('MONGO_COLLECTION', "redditstream")
    try:
        existing_cols = db.list_collection_names()
        if col_name not in existing_cols and "processed_posts" in existing_cols:
            col_name = "processed_posts"
    except Exception:
        pass
        
    fifty_days_ago = datetime.now() - timedelta(days=50)
    # Chuyển đổi định dạng vì created_at lưu dưới dạng chuỗi 'YYYY-MM-DD HH:MM:SS'
    fifty_days_ago_str = fifty_days_ago.strftime('%Y-%m-%d %H:%M:%S')
    mongo_raw = list(db[col_name].find({"created_at": {"$gte": fifty_days_ago_str}}))
    
    df_merged = merge_and_prep_data(binance_raw, mongo_raw)
    df_features = calculate_technical_indicators(df_merged)
    df_features.rename(columns={'Sentiment': 'Sentiment_Score'}, inplace=True)
    
    # Loại bỏ nến ngày hôm nay (chưa đóng cửa) để lấy nến ngày hôm qua (đóng lúc 7h sáng nay) làm nến cuối cùng
    df_features = df_features.iloc[:-1]
    
    feature_columns = [
        'Volume', 'Close', 'Open', 'High', 'Low', 'VWAP', 'Sentiment_Score'
        ]
    last_30_days = df_features[feature_columns].tail(30)
    
    return last_30_days.values.tolist()

def run_inference(features_list):
    """Load model"""
    if os.path.exists('/opt/airflow/models'):
        BASE_PATH = '/opt/airflow/models'
    else:
        BASE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
    
    import json
    from sklearn.preprocessing import MinMaxScaler
    
    with open(f'{BASE_PATH}/BTC_scaler_X.json', 'r') as f:
        scaler_X_data = json.load(f)
    feature_scaler = MinMaxScaler()
    feature_scaler.min_ = np.array(scaler_X_data['min_'])
    feature_scaler.scale_ = np.array(scaler_X_data['scale_'])
    feature_scaler.data_min_ = np.array(scaler_X_data['data_min_'])
    feature_scaler.data_max_ = np.array(scaler_X_data['data_max_'])

    with open(f'{BASE_PATH}/BTC_scaler_y.json', 'r') as f:
        scaler_y_data = json.load(f)
    target_scaler = MinMaxScaler()
    target_scaler.min_ = np.array(scaler_y_data['min_'])
    target_scaler.scale_ = np.array(scaler_y_data['scale_'])
    target_scaler.data_min_ = np.array(scaler_y_data['data_min_'])
    target_scaler.data_max_ = np.array(scaler_y_data['data_max_'])
    
    dataset = np.array(features_list)
    scaled_dataset = feature_scaler.transform(dataset)
    X_test_t = torch.tensor(np.array([scaled_dataset]), dtype=torch.float32)
    
    device = torch.device('cpu') 
    model = UltimatePaperLSTM(input_size=7, output_size=1)
    model.load_state_dict(torch.load(f'{BASE_PATH}/BTC_n30_num2_d0.2_l0.0001_b64_h128_test_lstm_model.pth', map_location=device))
    model.eval()

    with torch.no_grad():
        prediction = model(X_test_t).squeeze()
        if prediction.dim() == 0: prediction = prediction.unsqueeze(0)
        
    predicted_return = target_scaler.inverse_transform(prediction.cpu().numpy().reshape(-1, 1))[0][0]
    yesterday_close = features_list[-1][1]
    pred_usd = (1 + predicted_return) * yesterday_close
    
    # CHỈ TRẢ VỀ DỮ LIỆU SỐ CHO AIRFLOW
    return {
        "yesterday_close": float(yesterday_close),
        "pred_usd": float(pred_usd)
    }