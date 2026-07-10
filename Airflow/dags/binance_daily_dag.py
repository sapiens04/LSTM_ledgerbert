import os
import requests
from datetime import datetime, timedelta
from airflow.decorators import dag, task

from predict_daily import fetch_and_prepare_features, run_inference
from notifications_to_tele import send_telegram_alert


# local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
default_args = {
    'owner': 'dung_hust',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='daily_crypto_lstm_pipeline',
    default_args=default_args,
    schedule_interval='2 7 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['crypto', 'production']
)
def daily_prediction_pipeline():

    @task()
    def prepare_data():
        return fetch_and_prepare_features()

    @task()
    def predict_and_notify(features_list: list):
      
        res = run_inference(features_list)
        send_telegram_alert(res["yesterday_close"], res["pred_usd"])

    # THIẾT LẬP DÒNG CHẢY
    features = prepare_data()
    predict_and_notify(features)

crypto_pipeline = daily_prediction_pipeline()