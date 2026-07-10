import os
import requests
import logging

def send_telegram_alert(yesterday_close: float, pred_usd: float):
    """Hàm chuyên dụng để gửi thông báo Telegram"""
    TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
    
    if not TOKEN or not CHAT_ID:
        raise ValueError("Thiếu biến môi trường TELEGRAM_BOT_TOKEN hoặc TELEGRAM_CHAT_ID")
        
    diff = pred_usd - yesterday_close
    pct = (diff / yesterday_close) * 100
    icon = "BULLISH" if diff > 0 else "BEARISH"
    
    msg = (
        f"**[HUST AI] CẬP NHẬT BTC NGÀY MỚI**\n\n"
        f"Giá đóng cửa hôm qua: **${yesterday_close:,.2f}**\n"
        f"AI Dự đoán hôm nay: **${pred_usd:,.2f}**\n"
        f"Biến động: {icon} **{diff:+,.2f}** ({pct:+,.2f}%)\n"
    )
    
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage", 
            json={'chat_id': CHAT_ID, 'text': msg, 'parse_mode': 'Markdown'}
        )
        response.raise_for_status()
        logging.info("Gửi Telegram thành công!")
    except Exception as e:
        logging.error(f"Lỗi gửi Tele: {e}")