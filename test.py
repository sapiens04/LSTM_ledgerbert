import requests
import json

# Thay các thông tin này bằng của bạn (hoặc đọc trực tiếp từ biến môi trường)
RAPIDAPI_KEY = "9eb410e6edmsh9a568a4252e8f7ap12b2c4jsn386f9f31cf65"
RAPIDAPI_HOST = "reddit34.p.rapidapi.com"
SUBREDDIT = "Cryptocurrency"

def test_reddit_api():
    url = f"https://{RAPIDAPI_HOST}/getPostsBySubreddit"
    
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST
    }
    
    params = {
        "subreddit": SUBREDDIT,
        "sort": "new",
        "limit": 5 # Lấy thử 5 bài thôi cho gọn
    }
    
    print(f"🚀 Đang gọi API tới r/{SUBREDDIT}...")
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        print("✅ Thành công! Dữ liệu trả về:")
        
        # In ra cấu trúc sơ bộ để bạn nhìn xem đã khớp chưa
        posts = data.get("data", {}).get("posts", [])
        for i, item in enumerate(posts):
            post = item.get("data", {})
            print(f"\n--- Bài số {i+1} ---")
            print(f"Title: {post.get('title')}")
            print(f"Body: {post.get('selftext', post.get('description', ''))}")
    else:
        print(f"❌ Lỗi {response.status_code}: {response.text}")

if __name__ == "__main__":
    test_reddit_api()