import requests

def send_telegram_message(bot_token, chat_id, message):
    """
    Hàm gửi tin nhắn vào group Telegram thông qua Bot API.
    """
    # Thêm dòng này để debug (bắt lỗi)
    print(f"Nội dung chuẩn bị gửi đi: '{message}'") 
    
    if not message or str(message).strip() == "":
        print("Lỗi ở code của bạn: Nội dung tin nhắn đang trống rỗng!")
        return
    # Endpoint chuẩn của Telegram API
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    # Payload chứa thông tin gửi đi
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML" # Cho phép định dạng chữ đậm, nghiêng bằng thẻ HTML
    }
    
    try:
        # Thực hiện gọi API
        response = requests.post(api_url, json=payload, timeout=10)
        
        # Kiểm tra kết quả
        if response.status_code == 200:
            print("✅ Gửi tin nhắn thành công!")
            print("Chi tiết:", response.json())
        else:
            print(f"❌ Gửi thất bại. Mã lỗi: {response.status_code}")
            print("Chi tiết lỗi:", response.text)
            
    except Exception as e:
        print(f"⚠️ Lỗi kết nối: {e}")

# ==========================================
# THAY THÔNG TIN CỦA BẠN VÀO ĐÂY
# ==========================================
if __name__ == "__main__":
    TOKEN = "8918152423:AAEwB97ciLBc03ay6YbqbWyz4i5W2ZEg6oo"
    GROUP_CHAT_ID = "-1003922534709" # Nhớ giữ lại dấu trừ "-" nếu là group
    
    # Nội dung tin nhắn (Có thể dùng thẻ <b> cho in đậm, <i> cho in nghiêng)
    TEXT = """
    <b>🚨 CẢNH BÁO TỪ AIRFLOW 🚨</b>
    Mô hình LSTM đã huấn luyện xong.
    Độ chính xác: <i>95.5%</i>
    """
    
    send_telegram_message(TOKEN, GROUP_CHAT_ID, TEXT)