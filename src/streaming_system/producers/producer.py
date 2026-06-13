import os
import sys
# Tự động tìm và thêm thư mục gốc dự án vào danh sách tìm kiếm của Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Sau đó mới đến các dòng import cũ của bạn
from src.common.create_topic import create_topic
import logging

from src.common.create_topic import create_topic
from src.common.get_kafka_conf import create_kafka_producer
from src.common.settings import settings
from src.streaming_system.producers.reddit.reddit_stream import RedditRapidAPIStream

logging.basicConfig(level=logging.INFO)

create_topic(settings.KAFKA_TOPIC, 1, 1)

producer = create_kafka_producer()

reddit = RedditRapidAPIStream(
    settings.RAPIDAPI_KEY,
    settings.RAPIDAPI_HOST,
    settings.SUBREDDIT,
    settings.KAFKA_TOPIC,
    producer,
)

reddit.fetch_and_stream()
