import os
import json
import time
import logging
import requests
from dotenv import load_dotenv
from confluent_kafka import Producer

# Cấu hình log trực quan
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RedditRapidAPIStream:
    def __init__(self, rapidapi_key: str, rapidapi_host: str, subreddit: str, kafka_topic: str, kafka_producer: Producer) -> None:
        self.subreddit = subreddit
        self.kafka_topic = kafka_topic
        self.kafka_producer = kafka_producer
        
        # Cấu hình Header bắt buộc của RapidAPI
        self.headers = {
            "X-RapidAPI-Key": rapidapi_key,
            "X-RapidAPI-Host": rapidapi_host
        }
        # URL Endpoint lấy bài viết mới nhất của một Subreddit (Thay đổi tùy theo API cụ thể bạn mua trên RapidAPI, đây là định dạng phổ biến nhất)
        self.url = f"https://{rapidapi_host}/getPostsBySubreddit"
        # Set để lưu các ID bài viết đã gửi, tránh đẩy trùng lặp dữ liệu vào Kafka khi lặp vòng
        self.sent_submission_ids = set()

    def fetch_and_stream(self):
        logging.info(f"Bắt đầu luồng quét bài viết mới từ r/{self.subreddit}...")
        
        while True:
            try:
                # 1. Gọi đến đúng endpoint endpoint v1/subreddit/new hiển thị trên màn hình của bạn
                # Thêm tham số query 'subreddit' vào params nếu API yêu cầu truyền tên sub qua param
                params = {
                    "subreddit": self.subreddit, 
                    "sort": "new",
                    "limit": 100
                }
                response = requests.get(self.url, headers=self.headers, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # 2. BÓC TÁCH THEO ĐÚNG MÀN HÌNH: Lấy mảng nằm trong key 'body'
                    # submissions = data.get("body", [])
                    root_data = data.get("data", {}) 
                    posts_list = root_data.get("posts", [])
                    
                    new_count = 0
                    for item in posts_list:
                        submission = item.get("data", {})
                        sub_id = submission.get("id")
                        
                        # Kiểm tra trùng lặp để tránh xả data lặp vào Kafka
                        if sub_id and sub_id not in self.sent_submission_ids:
                            title = submission.get("title", "No Title") # Lấy title
                            
                            # THÊM DÒNG NÀY ĐỂ DEBUG:
                            logging.info(f"👉 Đang đẩy bài: {title[:10]}...")
                            submission_data = {
                                "id": sub_id,
                                "title": submission.get("title", ""),
                                "body": submission.get("selftext", submission.get("description", "")), # Dự phòng lấy description nếu selftext trống
                                "upvotes": submission.get("ups", 0),
                                "upvote_ratio": submission.get("upvote_ratio", 0.0),
                                "url": submission.get("url", ""),
                                "created_at": submission.get("created_utc", time.time()) # Nếu thiếu trường lấy tạm thời gian hiện tại
                            }
                            
                            # Ép sang kiểu JSON chuỗi và encode sang utf-8 cho Kafka
                            message = json.dumps(submission_data).encode("utf8")
                            
                            # Bắn trực tiếp vào Kafka Topic
                            self.kafka_producer.produce(topic=self.kafka_topic, value=message)
                            self.sent_submission_ids.add(sub_id)
                            new_count += 1
                            
                    if new_count > 0:
                        self.kafka_producer.flush()
                        logging.info(f"[RapidAPI] Đã đẩy thêm {new_count} bài viết mới từ r/{self.subreddit} vào Kafka.")
                        
                    if len(self.sent_submission_ids) > 5000:
                        self.sent_submission_ids.clear()
                        
                else:
                    logging.error(f"Lỗi API: Status {response.status_code} - {response.text}")
                    
            except Exception as e:
                logging.error(f"Lỗi xử lý luồng: {e}")
                
            sleep_time = 10
            logging.info(f"Đang đợi {sleep_time} giây cho đợt quét tiếp theo...")
            time.sleep(sleep_time)

if __name__ == "__main__":
    # Nạp các biến môi trường
    load_dotenv()
    
    # Đọc cấu hình bảo mật từ .env
    RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
    RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "reddit3.p.rapidapi.com") # Tên host cụ thể của API bạn chọn
    SUBREDDIT = os.getenv("SUBREDDIT", "crypto")
    KAFKA_TOPIC = os.getenv("KAFKA_REDDIT_TOPIC", "reddit-posts")
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    
    if not RAPIDAPI_KEY:
        logging.critical("Thiếu cấu hình RAPIDAPI_KEY trong file .env! Hệ thống dừng.")
        exit(1)
        
    # Khởi tạo Confluent Kafka Producer thật
    try:
        producer_config = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
        kafka_producer = Producer(producer_config)
        logging.info("Khởi tạo Kafka Producer thành công.")
    except Exception as e:
        logging.critical(f"Không thể kết nối tới Kafka Broker: {e}")
        exit(1)
        
    # Chạy luồng stream
    streamer = RedditRapidAPIStream(
        rapidapi_key=RAPIDAPI_KEY,
        rapidapi_host=RAPIDAPI_HOST,
        subreddit=SUBREDDIT,
        kafka_topic=KAFKA_TOPIC,
        kafka_producer=kafka_producer
    )
    
    streamer.fetch_and_stream()