import ccxt
import pandas as pd
import time
from datetime import datetime, timezone
from dateutil.parser import parse

class BinanceRangeFetcher:
    def __init__(self):
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
        })

    def to_ms(self, dt_str):
        """Chuyển đổi string ngày tháng sang timestamp ms (Ép về UTC)"""
        if isinstance(dt_str, str):
            # Ép về UTC để đồng bộ hoàn toàn với API Binance
            dt = parse(dt_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        return dt_str

    def fetch_ohlcv_range(self, symbol='BTC/USDT', timeframe='1h', start_date='2022-01-01', end_date='2022-12-31'):
        """
        Thu thập dữ liệu trong một khoảng thời gian xác định bằng CCXT.
        """
        since_ms = self.to_ms(start_date)
        until_ms = self.to_ms(end_date)
        
        all_data = []
        # Parse timeframe ra milliseconds (ví dụ '1h' -> 3600000 ms)
        candle_ms = self.exchange.parse_timeframe(timeframe) * 1000
        
        print(f"--- Thu thập {symbol} từ {start_date} đến {end_date} ---")

        while since_ms <= until_ms:
            try:
                # Gọi API thông qua CCXT
                ohlcv = self.exchange.fetch_ohlcv(
                    symbol=symbol, 
                    timeframe=timeframe, 
                    since=since_ms, 
                    limit=1000
                )

                if not ohlcv or len(ohlcv) == 0:
                    print("⚠️ Không nhận được dữ liệu thêm từ API. Dừng tiến trình.")
                    break
                
                # Lọc nến hợp lệ trực tiếp
                valid_candles = [candle for candle in ohlcv if candle[0] <= until_ms]
                if not valid_candles:
                    break
                    
                all_data.extend(valid_candles)

                # Hiển thị tiến độ dựa trên nến cuối cùng nhận được
                last_timestamp = ohlcv[-1][0]
                current_time_str = datetime.fromtimestamp(last_timestamp / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                print(f"-> Đã lấy đến ngày (UTC): {current_time_str} (Tổng cộng: {len(all_data)} nến)")

                # BẮT BUỘC: Tịnh tiến mốc thời gian tiếp theo dựa trên nến cuối cùng của kết quả trả về
                # Nếu nến cuối cùng vượt mốc kết thúc, vòng lặp while sẽ tự kết thúc ở lượt kế tiếp
                since_ms = last_timestamp + candle_ms

                # Nghỉ theo quy định của CCXT RateLimit
                time.sleep(self.exchange.rateLimit / 1000)

            except Exception as e:
                print(f"❌ Lỗi hệ thống: {e}")
                time.sleep(5)  # Gặp lỗi mạng thì nghỉ 5s rồi thử lại thay vì ngắt hệ thống luôn
                continue

        if not all_data:
            return pd.DataFrame()

        # 2. Chuyển đổi sang DataFrame
        df = pd.DataFrame(all_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        
        # Sắp xếp và chuẩn hóa kiểu dữ liệu
        df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col])

        return df

# --- Thực thi ---
if __name__ == "__main__":
    fetcher = BinanceRangeFetcher()
    
    # Lấy dữ liệu trọn vẹn năm 2022 với khung nến ngày (1d) hoặc 15m, 4h tùy chọn
    df_2022 = fetcher.fetch_ohlcv_range(
        symbol='BTC/USDT',
        timeframe='1d',        
        start_date='2022-01-01 00:00:00',
        end_date='2022-12-31 23:59:59'
    )

    if not df_2022.empty:
        print("\n📊 Kết quả thu thập dữ liệu thành công:")
        print(df_2022.info())
        print(df_2022.tail())
        
        # Lưu thẳng sang ổ NTFS Windows cho an toàn dữ liệu
        # df_2022.to_csv('/media/dung/New Volume/Study/DATN/ai_system/data/raw/btc_2022_1d.csv', index=False)