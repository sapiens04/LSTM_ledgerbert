import pandas as pd
import torch
import os
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from tqdm import tqdm
from typing import List, Tuple

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG
# ==========================================
MODEL_NAME = "ExponentialScience/LedgerBERT-Market-Sentiment"
_LABELS = ["neutral", "bearish", "bullish"]

_tokenizer = None
_model = None

def _load_model():
    global _tokenizer, _model
    if _tokenizer is None or _model is None:
        print(f"[*] Đang nạp mô hình {MODEL_NAME}...")
        device = "cuda:0" if torch.cuda.is_available() else "cpu"
        _tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
        _model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME).to(device)
        _model.eval()
        print(f"[*] Mô hình sẵn sàng trên: {device}")

# ==========================================
# 2. LOGIC XỬ LÝ SENTIMENT
# ==========================================
def estimate_sentiment_batch(texts: List[str], batch_size: int = 32) -> List[Tuple[float, str]]:
    if not texts: return []
    _load_model()
    device = next(_model.parameters()).device
    results = []

    for i in range(0, len(texts), batch_size):
        batch = [str(t) for t in texts[i : i + batch_size]]
        with torch.no_grad():
            tokens = _tokenizer(batch, return_tensors="pt", padding=True, truncation=True, max_length=512).to(device)
            outputs = _model(**tokens)
            probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)
            
            for probs in probabilities:
                best_idx = torch.argmax(probs).item()
                results.append((probs[best_idx].item(), _LABELS[best_idx]))
    return results

# ==========================================
# 3. LUỒNG THỰC THI (ĐÃ XÓA UNIX TIMESTAMP)
# ==========================================
def main(input_path: str, output_path: str):
    if not os.path.exists(input_path):
        print(f"[!] Không tìm thấy file: {input_path}")
        return

    df = pd.read_csv(input_path)
    print(f"[*] Đang phân tích {len(df)} bài đăng...")
    
    # 1. Chạy mô hình AI lấy Sentiment
    headlines = df['clean_content'].fillna("").tolist()
    analysis_results = estimate_sentiment_batch(headlines, batch_size=32)

    # 2. Gắn kết quả Sentiment vào DataFrame
    df['sentiment_label'] = [res[1] for res in analysis_results]
    df['sentiment_confidence'] = [res[0] for res in analysis_results]

    # 3. CHUYỂN ĐỔI VÀ XÓA CỘT UNIX
    if 'created' in df.columns:
        print("[*] Đang chuẩn hóa định dạng ngày tháng và dọn dẹp cột thừa...")
        # Tạo cột date chuẩn
        df['date'] = pd.to_datetime(df['created'], unit='s').dt.strftime('%Y-%m-%d')
        # Xóa thẳng tay cột created gốc
        df.drop(columns=['created'], inplace=True)
        
        # Đẩy cột 'date' lên vị trí đầu tiên trong file CSV
        cols = ['date'] + [col for col in df.columns if col != 'date']
        df = df[cols]

    # 4. Lưu file
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"[OK] Đã lưu kết quả phân tích sạch đẹp vào: {output_path}")

if __name__ == "__main__":
    INPUT_FILE = "data/bitcoin_dataset_processed.csv"
    OUTPUT_FILE = "data/bitcoin_final_sentiment_analysis.csv"
    main(INPUT_FILE, OUTPUT_FILE)