import os
from pymongo import MongoClient

def inspect_mongodb():
    mongo_uri = os.getenv('MONGO_URI', "mongodb://root:123456@mongodb:27017/?authSource=admin")
    if not os.path.exists('/.dockerenv') and 'mongodb:27017' in mongo_uri:
        mongo_uri = mongo_uri.replace('mongodb:27017', 'localhost:27018')
        
    print(f"🔍 Đang kết nối tới MongoDB tại {mongo_uri}...")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
    
    try:
        # 1. Liệt kê các Database đang có
        dbs = client.list_database_names()
        print(f"\n✅ Kết nối thành công! Danh sách các Database hiện có trên Mongo:")
        for db_name in dbs:
            print(f"   - {db_name}")
            
        # 2. Quét sâu vào các Database liên quan đến dự án
        target_dbs = ['redditstream_db', 'crypto_db']
        found_data = False
        
        for db_name in target_dbs:
            if db_name in dbs:
                found_data = True
                print(f"\n📊 Chi tiết Database '{db_name}':")
                db = client[db_name]
                cols = db.list_collection_names()
                
                if not cols:
                    print("   (Database này trống, chưa có collection nào)")
                    continue
                    
                for col_name in cols:
                    col = db[col_name]
                    doc_count = col.count_documents({})
                    print(f"   - Collection '{col_name}': Chứa {doc_count:,} bản ghi.")
                    
                    if doc_count > 0:
                        print("     [Mẫu bản ghi mới nhất]:")
                        # Lấy bản ghi mới nhất để xem cấu trúc và ngày tháng
                        sample = col.find_one(sort=[('_id', -1)])
                        if sample:
                            # In ra các trường quan trọng (giới hạn ký tự nếu dài quá)
                            for k, v in list(sample.items()):
                                val_str = str(v)
                                if len(val_str) > 100:
                                    val_str = val_str[:100] + "..."
                                print(f"       * {k}: {val_str}")
                            print("       --------------------")
        
        if not found_data:
            print("\n⚠️ Không tìm thấy database 'redditstream_db' hay 'crypto_db'.")
            print("Có vẻ MongoDB đang chạy là cơ sở dữ liệu mới (chưa có dữ liệu Reddit cũ).")
            
    except Exception as e:
        print(f"\n❌ Lỗi kết nối tới MongoDB: {e}")
        print("Mẹo khắc phục:")
        print("1. Hãy chắc chắn container MongoDB của bạn đang chạy (chạy lệnh: docker ps)")
        print("2. Đảm bảo port 27018 đã được map thành công từ Docker ra máy host.")

if __name__ == "__main__":
    inspect_mongodb()
