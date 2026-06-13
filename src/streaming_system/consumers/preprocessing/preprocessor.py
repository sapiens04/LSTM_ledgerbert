from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, from_unixtime, udf
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from src.streaming_system.consumers.preprocessing.clean_text import CleanText

class Preprocessor:
    def __init__(
        self,
        clean_text: CleanText,
    ) -> None:
        self.clean_text = clean_text

    def _convert_date_format(self, df: DataFrame) -> DataFrame:
        return df.withColumn("created_at", from_unixtime(df["created_at"]))

    def _create_content(self, df: DataFrame) -> DataFrame:
        # Dùng concat_ws để tránh mất dữ liệu khi title hoặc body bị null
        return df.withColumn("content", concat_ws(" ", col("title"), col("body")))

    def _clean_text(self, df: DataFrame) -> DataFrame:
        clean_text_udf = udf(self.clean_text.clean_text, StringType())
        return df.withColumn("cleaned_content", clean_text_udf(col("content")))

    def _generate_ledgerbert_sentiment(self, df: DataFrame) -> DataFrame:
        """
        Sử dụng UDF trả về kiểu StructType để lấy cả Label và Điểm số 
        chỉ trong 1 lần gọi GPU duy nhất.
        """
        # Định nghĩa cấu trúc trả về
        result_schema = StructType([
            StructField("label", StringType(), False),
            StructField("confidence", FloatType(), False)
        ])

        @udf(returnType=result_schema)
        def ledgerbert_udf(text: str):
            if not text or len(text.strip()) < 10:
                return {"label": "neutral", "confidence": 0.0}
            
            # Kỹ thuật Lazy Load
            global _bert_pipeline
            if '_bert_pipeline' not in globals():
                from transformers import pipeline
                import torch
                
                # Tự động nhận diện GPU NVIDIA
                device_id = 0 if torch.cuda.is_available() else -1
                
                _bert_pipeline = pipeline(
                    task="text-classification", 
                    model="ExponentialScience/LedgerBERT-Market-Sentiment", 
                    device=device_id, 
                    max_length=512, 
                    truncation=True
                )
            
            try:
                # Trả về kết quả dự đoán dạng dict
                result = _bert_pipeline(text[:512])[0]
                return {
                    "label": str(result['label']).lower(),
                    "confidence": float(result['score'])
                }
            except Exception as e:
                print(f"[*] Lỗi suy luận BERT: {e}")
                return {"label": "neutral", "confidence": 0.0}

        # Tạo cột tạm chứa kết quả dict
        df = df.withColumn("bert_result", ledgerbert_udf(col("cleaned_content")))

        # Tách dict thành 2 cột độc lập và xóa cột tạm
        df = df.withColumn("sentiment", col("bert_result.label")) \
               .withColumn("sentiment_confidence", col("bert_result.confidence")) \
               .drop("bert_result")

        return df

    def preprocess(self, df: DataFrame) -> DataFrame:
        # Luồng chạy giữ nguyên sự tuần tự, rõ ràng
        df = self._convert_date_format(df)
        df = self._create_content(df)
        df = self._clean_text(df)
        
        # Gọi 1 bước AI duy nhất
        df = self._generate_ledgerbert_sentiment(df)
        
        # Dọn dẹp mạnh tay các cột không cần thiết để tối ưu dung lượng MongoDB
        df = df.drop("content", "title", "body", "cleaned_content", "url")
        return df