import os
import sys

# Khắc phục lỗi tương thích khi load scaler/model từ numpy 2.x sang numpy 1.x
try:
    import numpy._core
except ImportError:
    import numpy.core as _core
    sys.modules['numpy._core'] = _core
    import numpy.core.multiarray as _multiarray
    sys.modules['numpy._core.multiarray'] = _multiarray
    import numpy.core.numeric as _numeric
    sys.modules['numpy._core.numeric'] = _numeric

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
class UltimatePaperBiLSTM(nn.Module):
    def __init__(self, input_size=20, output_size=1, dropout_rate=0.2):
        super(UltimatePaperBiLSTM, self).__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=50,
            num_layers=2,
            batch_first=True
        )
        self.dropout = nn.Dropout(dropout_rate)
        self.fc = nn.Linear(50, output_size)

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
    
    feature_columns = [
        'Volume', 'Year', 'Month', 'Day', 'MA5', 'MA10', 'MA20', 'RSI', 'MACD',
        'VWAP', 'SMA', 'Std_dev', 'Upper_band', 'Lower_band', 'ATR',
        'Close_yes', 'Open_yes', 'High_yes', 'Low_yes', 'Sentiment'
        ]
    last_10_days = df_features[feature_columns].tail(10)
    
    return last_10_days.values.tolist()

def run_inference(features_list):
    """Load model"""
    if os.path.exists('/opt/airflow/models'):
        BASE_PATH = '/opt/airflow/models'
    else:
        BASE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
    
    feature_scaler = joblib.load(f'{BASE_PATH}/BTC_s1_n10_scaler_X.pkl')
    target_scaler = joblib.load(f'{BASE_PATH}/BTC_s1_n10_scaler_y.pkl')
    
    dataset = np.array(features_list)
    scaled_dataset = feature_scaler.transform(dataset)
    X_test_t = torch.tensor(np.array([scaled_dataset]), dtype=torch.float32)
    
    device = torch.device('cpu') 
    model = UltimatePaperBiLSTM(input_size=20).to(device)
    model.load_state_dict(torch.load(f'{BASE_PATH}/BTC_s1_n10_lstm_model.pth', map_location=device))
    model.eval()

    with torch.no_grad():
        prediction = model(X_test_t).squeeze()
        if prediction.dim() == 0: prediction = prediction.unsqueeze(0)
        
    predicted_return = target_scaler.inverse_transform(prediction.cpu().numpy().reshape(-1, 1))[0][0]
    yesterday_close = features_list[-1][15]
    pred_usd = (1 + predicted_return) * yesterday_close
    
    # CHỈ TRẢ VỀ DỮ LIỆU SỐ CHO AIRFLOW
    return {
        "yesterday_close": float(yesterday_close),
        "pred_usd": float(pred_usd)
    }