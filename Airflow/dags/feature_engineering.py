import pandas as pd
import numpy as np

def calculate_technical_indicators(data: pd.DataFrame) -> pd.DataFrame:
    """
    Bộ tiền xử lý cô đọng cho BTC: Sử dụng OHLCV + Sentiment.
    Đầu vào (data): DataFrame chứa ít nhất các cột Open, High, Low, Close, Volume.
    """
    # Tránh làm thay đổi data gốc bên ngoài
    df = data.copy()

    # Bước 0: Chuẩn hóa chữ hoa đầu từ cho các cột
    df.columns = [c.capitalize() for c in df.columns]

    # Đảm bảo cột Date làm Index
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
    elif df.index.name and df.index.name.lower() == 'date':
        df.index = pd.to_datetime(df.index)
        df.index.name = 'Date'

    # 1. Thêm đặc trưng ngày tháng
    df['Year'] = df.index.year
    df['Month'] = df.index.month
    df['Day'] = df.index.day

    # 2. Đường trung bình động (MA)
    df['MA5'] = df['Close'].shift(1).rolling(window=5).mean()
    df['MA10'] = df['Close'].shift(1).rolling(window=10).mean()
    df['MA20'] = df['Close'].shift(1).rolling(window=20).mean()


    # 3. Chỉ báo RSI
    delta = df['Close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df['RSI'] = 100 - (100 / (1 + rs))

    # 4. Chỉ báo MACD
    exp1 = df['Close'].ewm(span=12, adjust=False).mean()
    exp2 = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = exp1 - exp2
    df['Signal_Line'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['MACD_Histogram'] = df['MACD'] - df['Signal_Line']

    # 5. Chỉ báo VWAP
    df['VWAP'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()

    # 6. Dải Bollinger Bands
    period = 20
    df['SMA'] = df['Close'].rolling(window=period).mean()
    df['Std_dev'] = df['Close'].rolling(window=period).std()
    df['Upper_band'] = df['SMA'] + 2 * df['Std_dev']
    df['Lower_band'] = df['SMA'] - 2 * df['Std_dev']

    # 7. Chỉ báo ROC & 8. Chỉ báo ATR
    df['ROC'] = df['Close'].pct_change(periods=1) * 100
    
    high_low_range = df['High'] - df['Low']
    high_close_range = abs(df['High'] - df['Close'].shift(1))
    low_close_range = abs(df['Low'] - df['Close'].shift(1))
    true_range = pd.concat([high_low_range, high_close_range, low_close_range], axis=1).max(axis=1)
    df['ATR'] = true_range.rolling(window=14).mean()

    # 9. Dữ liệu hành vi giá của ngày hôm trước
    df[['Close_yes', 'Open_yes', 'High_yes', 'Low_yes']] = df[['Close', 'Open', 'High', 'Low']].shift(1)

    # LƯU Ý CHÍ MẠNG: Giữ nguyên lệnh dropna() này cho tập train, 
    # nhưng khi chạy Airflow phải cực kỳ cẩn thận (xem giải thích bên dưới)
    df = df.dropna()

    return df