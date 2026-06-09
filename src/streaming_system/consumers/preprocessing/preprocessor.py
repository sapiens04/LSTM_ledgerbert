from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, from_unixtime, udf
from pyspark.sql.types import StringType

from src.consumer.preprocessing.clean_text import CleanText

class Preprocessor:
    def __init__(self, clean_text: CleanText) -> None:
        # Chúng ta chỉ cần CleanText. Các model AI sẽ được nạp trực tiếp bằng Lazy Load bên dưới
        self.clean_text = clean_text

    def _convert_date_format(self, df: DataFrame) -> DataFrame:
        # Chuyển đổi timestamp của Unix thành ngày tháng đọc được
        return df.withColumn("created_at", from_unixtime(col("created_at")))

    def _create_content(self, df: DataFrame) -> DataFrame:
        # Dùng concat_ws (" ") thay vì concat() để nếu body bị null thì title không bị mất
        return df.withColumn("content", concat_ws(" ", col("title"), col("body")))

    def _clean_text_for_bert(self, df: DataFrame) -> DataFrame:
        # CHÚ Ý: Gọi đúng hàm clean_for_bert_lstm để KHÔNG xóa stopwords, giữ nguyên ngữ cảnh
        clean_text_udf = udf(self.clean_text.clean_for_bert_lstm, StringType())
        return df.withColumn("cleaned_content", clean_text_udf(col("content")))

    def _predict_with_ledgerbert(self, df: DataFrame) -> DataFrame:
        """
        Sử dụng Lazy Loading UDF để phân tích AI trên luồng Spark Streaming
        """
        @udf(returnType=StringType())
        def ledgerbert_udf(text: str) -> str:
            if not text or len(text.strip()) < 10:
                return "neutral"
            
            # Kỹ thuật Lazy Load: Chỉ tải model 1 lần duy nhất cho mỗi Worker
            global _bert_pipeline
            if '_bert_pipeline' not in globals():
                from transformers import pipeline
                
                # Ép device=-1 (CPU) để bảo vệ VRAM của Card màn hình không bị tràn
                # khi Spark chạy đa luồng trên môi trường Local.
                _bert_pipeline = pipeline(
                    task="text-classification", 
                    model="ExponentialScience/LedgerBERT-Market-Sentiment", 
                    device=-1, 
                    max_length=512, 
                    truncation=True
                )
            
            try:
                # Cắt độ dài phòng hờ và lấy kết quả
                # Pipeline sẽ trả về list chứa dict: [{'label': 'bullish', 'score': 0.95}]
                result = _bert_pipeline(text[:512])[0]
                
                # Ép về chữ thường để chuẩn hóa dữ liệu lưu vào MongoDB
                return str(result['label']).lower()
            except Exception as e:
                print(f"[*] Lỗi suy luận BERT: {e}")
                return "neutral"

        # Đổi tên cột thành 'sentiment_label' cho khớp với file Pandas Batch của bạn
        return df.withColumn("sentiment_label", ledgerbert_udf(col("cleaned_content")))
    def preprocess(self, df: DataFrame) -> DataFrame:
        # 1. Định dạng ngày tháng
        df = self._convert_date_format(df)
        
        # 2. Gom chữ
        df = self._create_content(df)
        
        # 3. Làm sạch chữ (Giữ ngữ cảnh)
        df = self._clean_text_for_bert(df)
        
        # 4. Suy luận bằng trí tuệ nhân tạo (LedgerBERT)
        df = self._predict_with_ledgerbert(df)
        
        # 5. Dọn dẹp Dataframe trước khi ghi vào Database
        # Cắt bỏ những cột nguyên liệu thô để tối ưu dung lượng ổ cứng MongoDB
        df = df.drop("content", "title", "body")
        
        return df