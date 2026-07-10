import json
import time
import random
import string
import logging
from kafka import KafkaProducer
import os

# --- CẤU HÌNH ---
KAFKA_BROKER = 'localhost:9092' # Sửa lại nếu Kafka chạy trên port/IP khác
TOPIC_NAME = 'reddit-posts-sim'
TOTAL_RECORDS = 1000000 # Tổng số data muốn test hiệu năng

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def generate_reddit_id():
    """Tạo ID mới chuẩn Reddit để nhân bản data vô hạn mà không trùng"""
    return "t3_" + ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

def load_base_data():
    """Đọc file khuôn mẫu tải từ Mockaroo"""
    try:
        base_path = os.path.join(os.path.dirname(__file__), 'reddit_base.json')
        with open(base_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error("Không tìm thấy file 'reddit_base.json'. Vui lòng tải từ Mockaroo.")
        exit(1)

def main():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks=0,                  # Không đợi Kafka phản hồi, cứ bắn liên tục (Fire-and-forget)
        linger_ms=10,            # Chờ 10ms để gom nhiều tin nhắn thành một lô (batch) lớn rồi mới gửi
        batch_size=32768,        # Kích thước tối đa của một lô là 32KB
        compression_type='gzip'  # Nén dữ liệu bằng GZIP để truyền qua mạng nhanh hơn
    )
    
    base_data = load_base_data()
    logging.info(f"--- BẮT ĐẦU STREAMING VÀO KAFKA TOPIC: {TOPIC_NAME} ---")

    sent_count = 0
    
    while sent_count < TOTAL_RECORDS:
        sent_count += 1
        
        # 1. Bốc ngẫu nhiên 1 bản ghi từ Mockaroo để làm nội dung nền
        template = random.choice(base_data)
        
        # 2. Lắp ID mới và thời gian thực để tạo ra 1 gói tin hoàn toàn mới
        sub_id = generate_reddit_id()
        submission_data = {
            "id": sub_id,
            "title": template.get("title", f"Bài phân tích {sent_count}"),
            "body": template.get("body", "Nội dung thảo luận mặc định..."),
            "upvotes": template.get("upvotes", random.randint(10, 10000)),
            "upvote_ratio": template.get("upvote_ratio", round(random.uniform(0.6, 1.0), 2)),
            "url": f"https://www.reddit.com/r/dataengineering/comments/{sub_id}/",
            "created_at": time.time()
        }

        # 3. TIÊM LỖI ĐỂ TEST ĐƯỜNG ỐNG (FAULT INJECTION)
        
        # Cứ chạy được 1000 dòng thì cố tình gửi trùng ID 1 lần
        if sent_count % 1000 == 0:
            producer.send(TOPIC_NAME, submission_data)
            producer.send(TOPIC_NAME, submission_data) # Bắn đúp
            logging.warning(f"⚠️ [TEST LẶP] Đã bắn đúp bài: {submission_data['title'][:20]}...")
            continue
            
        # Cứ chạy được 1500 dòng thì xóa trắng nội dung (Null Body)
        if sent_count % 1500 == 0:
            submission_data["body"] = None
            producer.send(TOPIC_NAME, submission_data)
            logging.warning(f"⚠️ [TEST NULL] Đã bắn bài rỗng nội dung: {sub_id}")
            continue

        # 4. GỬI DATA CHUẨN
        producer.send(TOPIC_NAME, submission_data)
        
        if sent_count % 500 == 0:
            logging.info(f"👉 Đang đẩy trơn tru... Đã hoàn tất {sent_count}/{TOTAL_RECORDS} bản ghi.")
            
        # Nghỉ 0.05 giây để mô phỏng tốc độ API thật (Tương đương 20 req/giây)
        # Có thể chỉnh xuống 0.01 để test chịu tải (Stress test)
        # time.sleep(0.01)

    # Đảm bảo toàn bộ data trong buffer được xả hết vào Kafka
    producer.flush()
    logging.info("--- HOÀN TẤT QUÁ TRÌNH PUSH DATA! ---")

if __name__ == "__main__":
    main()