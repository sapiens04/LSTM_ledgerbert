import re
import html
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize

class CleanText:
    def __init__(self) -> None:
        # Nạp thư viện NLTK dùng cho luồng xử lý Traditional ML
        self.stop_words = set(stopwords.words("english"))
        self.porter_stemmer = PorterStemmer()

    def _remove_reddit_noise(self, text: str) -> str:
        """
        Kế thừa logic từ luồng Pandas: Xóa sạch rác đặc thù của Reddit
        """
        if not isinstance(text, str) or text.lower() in ['[removed]', '[deleted]', 'nan']:
            return ""
        
        # 1. Giải mã HTML entities (VD: &amp; -> &)
        text = html.unescape(text)
        
        # 2. Xử lý Markdown Links: Giữ lại text, bỏ URL. (VD: [click](http...) -> click)
        text = re.sub(r'\[(?P<txt>[^\]]+)\]\(https?://[^\s]+\)', r'\g<txt>', text)
        
        # 3. Xóa các URL thô còn sót lại
        text = re.sub(r'https?://[^\s]+', '', text)
        
        # 4. Dọn dẹp khoảng trắng, ký tự ngắt dòng
        text = text.replace("\n", " ").replace("\r", " ")
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()

    def _convert_to_lower(self, text: str) -> str:
        return text.lower()

    def _remove_unicode(self, text: str) -> str:
        # Tinh chỉnh lại Regex để không ăn nhầm các ký tự quan trọng của Crypto như $ (VD: $BTC)
        text = re.sub(r"(@[A-Za-z0-9_]+)|([^0-9A-Za-z \t\$\%\.\,])|^rt", "", text)
        return text

    def _remove_stop_words(self, text: str) -> str:
        text = " ".join(word for word in text.split() if word not in self.stop_words)
        return text

    def _stem_words(self, text: str) -> str:
        words = word_tokenize(text)
        stemmed_words = [self.porter_stemmer.stem(word) for word in words]
        return " ".join(stemmed_words)

    def clean_for_bert_lstm(self, text: str) -> str:
        """
        [KHUYÊN DÙNG CHO ĐỒ ÁN]
        Dành riêng cho các mô hình ngữ cảnh sâu (FinBERT, LedgerBERT, LSTM).
        Chỉ dọn rác Reddit, giữ nguyên Stopwords, không Stemming để bảo toàn ngữ cảnh.
        """
        text = self._remove_reddit_noise(text)
        return text

    def clean_for_traditional_ml(self, text: str) -> str:
        """
        Dành cho các mô hình đếm tần suất từ (TF-IDF, Naive Bayes, Random Forest).
        Cắt cụt từ và xóa từ nối để giảm chiều dữ liệu.
        """
        text = self._remove_reddit_noise(text)
        text = self._convert_to_lower(text)
        text = self._remove_unicode(text)
        text = self._remove_stop_words(text)
        text = self._stem_words(text)
        return text

if __name__ == "__main__":
    cleantext = CleanText()
    
    sample_text = "Hey guys, check out this post [Binance News](https://binance.com) &amp; let me know! $BTC is pumping, but I think it's a trap. [removed]"
    
    print(cleantext.clean_for_bert_lstm(sample_text))