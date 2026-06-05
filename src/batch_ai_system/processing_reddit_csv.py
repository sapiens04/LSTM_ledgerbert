import pandas as pd
import os
import re
import html
import glob
from tqdm import tqdm

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else '.'
INPUT_PATH = os.path.join(BASE_DIR, "config/data/reddit_data_2022")
OUTPUT_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_FILENAME = "bitcoin_dataset_processed.csv"

BTC_REGEX = r'\b(btc|bitcoin)\b'

def clean_reddit_content(text: str) -> str:
    if not isinstance(text, str) or text.lower() in ['[removed]', '[deleted]', 'nan']:
        return ""
    text = html.unescape(text)
    text = re.sub(r'\[(?P<txt>[^\]]+)\]\(https?://[^\s]+\)', r'\g<txt>', text)
    text = re.sub(r'https?://[^\s]+', '', text)
    text = text.replace("\n", " ").replace("\r", " ")
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def is_btc_related(text: str) -> bool:
    if not text: return False
    return bool(re.search(BTC_REGEX, text, flags=re.IGNORECASE))

def run_extraction_pipeline():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"[*] Đã tạo thư mục lưu trữ: {OUTPUT_DIR}")

    all_files = glob.glob(os.path.join(INPUT_PATH, "*.csv"))
    print(f"[*] Tìm thấy {len(all_files)} file dữ liệu thô.")

    processed_chunks = []

    for file in tqdm(all_files, desc="Đang quét và lọc BTC"):
        try:
            df = pd.read_csv(file, low_memory=False)
            df['full_raw'] = df['title'].fillna('') + " " + df['selftext'].fillna('')
            
            # Lọc nhanh bằng Regex trước
            df_btc = df[df['full_raw'].apply(is_btc_related)].copy()
            
            if not df_btc.empty:
                # Làm sạch sâu sau
                df_btc['clean_content'] = df_btc['full_raw'].apply(clean_reddit_content)
                df_btc = df_btc[df_btc['clean_content'].str.len() > 15]
                
                keep_cols = ['created', 'author', 'title', 'clean_content', 'score', 'num_comments', 'subreddit']
                existing_cols = [c for c in keep_cols if c in df_btc.columns]
                processed_chunks.append(df_btc[existing_cols])
        except Exception as e:
            print(f"[!] Lỗi file {os.path.basename(file)}: {e}")

    if processed_chunks:
        final_df = pd.concat(processed_chunks, ignore_index=True)
        if 'created' in final_df.columns:
            final_df = final_df.sort_values(by='created')

        output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)
        final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"[OK] Đã lưu file sạch tại: {output_path}")
    else:
        print("[!] Không tìm thấy dữ liệu BTC.")

if __name__ == "__main__":
    run_extraction_pipeline()